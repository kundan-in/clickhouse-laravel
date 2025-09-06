<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Query\Grammars\Grammar;

/**
 * ClickHouse Query Grammar
 *
 * This class extends Laravel's Grammar class to provide ClickHouse-specific
 * SQL query compilation and handles ClickHouse limitations.
 */
class ClickHouseQueryGrammar extends Grammar
{
    /**
     * Compile a select query for ClickHouse.
     *
     * @param  mixed  $query  The query builder instance
     * @return string
     */
    public function compileSelect($query): string
    {
        $sql = parent::compileSelect($query);

        // Optional: modify SQL for ClickHouse-specific syntax
        return $sql;
    }

    /**
     * Wrap a table in keyword identifiers and prepend database name.
     *
     * @param  mixed  $table
     * @param  string|null  $prefix
     * @return string
     */
    public function wrapTable($table, $prefix = null): string
    {
        // Handle Expression objects and complex table references first
        if ($table instanceof \Illuminate\Database\Query\Expression) {
            return $table->getValue($this);
        }

        // If table is already a string with database prefix, use parent method directly
        if (is_string($table) && str_contains($table, '.')) {
            return parent::wrapTable($table, $prefix);
        }

        // For simple table names, add database prefix if available
        if (is_string($table) && $this->connection) {
            $database = $this->connection->getDatabaseName();
            if ($database) {
                // Create database.table format and wrap it
                $fullTableName = $database.'.'.$table;

                return parent::wrapTable($fullTableName, $prefix);
            }
        }

        // Default to parent behavior
        return parent::wrapTable($table, $prefix);
    }

    /**
     * Compile a limit clause for ClickHouse.
     *
     * @param  mixed  $query  The query builder instance
     * @param  int  $limit  The limit value
     * @return string
     */
    public function compileLimit($query, $limit): string
    {
        return 'LIMIT '.(int) $limit;
    }

    /**
     * Compile an insert statement for ClickHouse.
     *
     * @param  mixed  $query  The query builder instance
     * @param  array  $values  The values to insert
     * @return string
     */
    public function compileInsert($query, array $values): string
    {
        if (empty($values)) {
            return '';
        }

        $table = $this->wrapTable($query->from);
        $columns = implode(', ', array_map([$this, 'wrap'], array_keys($values[0])));
        $placeholders = implode(', ', array_map(function ($row) {
            return '('.implode(', ', array_map([$this, 'parameter'], $row)).')';
        }, $values));

        return "INSERT INTO {$table} ({$columns}) VALUES {$placeholders}";
    }

    /**
     * Compile an update statement for ClickHouse.
     *
     * Note: ClickHouse does not support traditional UPDATE queries.
     * This method compiles to ALTER TABLE ... UPDATE syntax.
     *
     * @param  mixed  $query  The query builder instance
     * @param  array  $values  The values to update
     * @return string
     */
    public function compileUpdate($query, array $values): string
    {
        $table = $this->wrapTable($query->from);
        $where = $this->compileClickHouseWheres($query);

        if (empty($where)) {
            throw new \Exception('UPDATE queries on ClickHouse require a WHERE clause for safety.');
        }

        $sets = [];
        foreach ($values as $column => $value) {
            $sets[] = $this->wrap($column).' = '.$this->quoteClickHouseValue($value);
        }

        $setClause = implode(', ', $sets);

        return "ALTER TABLE {$table} UPDATE {$setClause} {$where}";
    }

    /**
     * Compile a delete statement for ClickHouse.
     *
     * Note: ClickHouse does not support traditional DELETE queries.
     * This method compiles to ALTER TABLE ... DELETE syntax.
     *
     * @param  mixed  $query  The query builder instance
     * @return string
     */
    public function compileDelete($query): string
    {
        $table = $this->wrapTable($query->from);
        $where = $this->compileClickHouseWheres($query);

        if (empty($where)) {
            throw new \Exception('DELETE queries on ClickHouse require a WHERE clause for safety.');
        }

        return "ALTER TABLE {$table} DELETE {$where}";
    }

    /**
     * Compile where clauses for ClickHouse ALTER statements.
     * ClickHouse ALTER statements don't support parameter placeholders,
     * so we need to embed values directly.
     *
     * @param  mixed  $query  The query builder instance
     * @return string
     */
    protected function compileClickHouseWheres($query): string
    {
        if (empty($query->wheres)) {
            return '';
        }

        $wheres = [];

        foreach ($query->wheres as $where) {
            $method = 'whereClickHouse'.$where['type'];
            if (method_exists($this, $method)) {
                $wheres[] = $this->{$method}($query, $where);
            } else {
                // Fallback to basic where handling
                $wheres[] = $this->whereClickHouseBasic($query, $where);
            }
        }

        if (count($wheres) > 1) {
            return 'WHERE '.implode(' '.strtolower($query->wheres[0]['boolean'] ?? 'and').' ', $wheres);
        }

        return count($wheres) > 0 ? 'WHERE '.$wheres[0] : '';
    }

    /**
     * Compile a basic where clause for ClickHouse ALTER statements.
     *
     * @param  mixed  $query
     * @param  array  $where
     * @return string
     */
    protected function whereClickHouseBasic($query, array $where): string
    {
        $column = $this->wrap($where['column']);
        $operator = $where['operator'];
        $value = $this->quoteClickHouseValue($where['value']);

        return "{$column} {$operator} {$value}";
    }

    /**
     * Compile a date where clause for ClickHouse ALTER statements.
     *
     * @param  mixed  $query
     * @param  array  $where
     * @return string
     */
    protected function whereClickHouseDate($query, array $where): string
    {
        $column = $this->wrap($where['column']);
        $value = $this->quoteClickHouseValue($where['value']);

        return "date({$column}) = {$value}";
    }

    /**
     * Quote a value for direct embedding in ClickHouse SQL.
     *
     * @param  mixed  $value
     * @return string
     */
    protected function quoteClickHouseValue($value): string
    {
        if (is_null($value)) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if (is_numeric($value)) {
            return (string) $value;
        }

        // Comprehensive string escaping for ClickHouse to prevent SQL injection
        $value = (string) $value;
        $escaped = str_replace([
            '\\',    // Backslash must be escaped first
            "'",     // Single quotes
            '"',     // Double quotes
            "\n",    // Newlines
            "\r",    // Carriage returns
            "\t",    // Tabs
            "\0",    // Null bytes
            "\x1a",  // EOF character
        ], [
            '\\\\',
            "\\'",
            '\\"',
            '\\n',
            '\\r',
            '\\t',
            '\\0',
            '\\Z',
        ], $value);

        return "'".$escaped."'";
    }
}
