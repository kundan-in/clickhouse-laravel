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
     * Use ALTER TABLE ... UPDATE for updating data.
     *
     * @param  mixed  $query  The query builder instance
     * @param  array  $values  The values to update
     *
     * @throws \Exception
     */
    public function compileUpdate($query, array $values): void
    {
        throw new \Exception('ClickHouse does not support standard UPDATE queries. Use ALTER TABLE ... UPDATE instead.');
    }

    /**
     * Compile a delete statement for ClickHouse.
     *
     * Note: ClickHouse does not support traditional DELETE queries.
     * Use ALTER TABLE ... DELETE for deleting data.
     *
     * @param  mixed  $query  The query builder instance
     *
     * @throws \Exception
     */
    public function compileDelete($query): void
    {
        throw new \Exception('ClickHouse does not support standard DELETE queries. Use ALTER TABLE ... DELETE instead.');
    }
}
