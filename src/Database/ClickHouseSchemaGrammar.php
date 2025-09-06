<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Schema\Grammars\Grammar;

/**
 * ClickHouse Schema Grammar
 *
 * This class extends Laravel's Schema Grammar class to provide ClickHouse-specific
 * DDL (Data Definition Language) operations and schema management.
 */
class ClickHouseSchemaGrammar extends Grammar
{
    /**
     * Compile a create table command.
     *
     * @param  mixed  $blueprint  The table blueprint
     * @param  mixed  $command  The create command
     * @return string
     */
    public function compileCreate($blueprint, $command): string
    {
        $sql = $this->compileCreateTable($blueprint);

        // Add engine specification
        if (isset($blueprint->engine)) {
            $sql .= $this->compileEngine($blueprint);
        } else {
            $sql .= ' ENGINE = MergeTree()';
        }

        // Add order by clause (required for MergeTree engines)
        if ($this->hasCommand($blueprint, 'orderBy')) {
            $sql .= $this->compileOrderBy($blueprint, $this->getCommandByName($blueprint, 'orderBy'));
        }

        // Add partition by clause
        if ($this->hasCommand($blueprint, 'partitionBy')) {
            $sql .= $this->compilePartitionBy($blueprint, $this->getCommandByName($blueprint, 'partitionBy'));
        }

        // Add primary key clause
        if ($this->hasCommand($blueprint, 'primaryKey')) {
            $sql .= $this->compilePrimaryKey($blueprint, $this->getCommandByName($blueprint, 'primaryKey'));
        }

        return $sql;
    }

    /**
     * Compile the basic create table clause.
     *
     * @param  mixed  $blueprint
     * @return string
     */
    protected function compileCreateTable($blueprint): string
    {
        return sprintf('CREATE TABLE %s (%s)',
            $this->wrapTable($blueprint),
            implode(', ', $this->getColumns($blueprint))
        );
    }

    /**
     * Compile engine clause.
     *
     * @param  mixed  $blueprint
     * @return string
     */
    protected function compileEngine($blueprint): string
    {
        $engine = $blueprint->engine ?? 'MergeTree';
        $options = $blueprint->engineOptions ?? [];

        $engineClause = " ENGINE = {$engine}";

        // Handle engine-specific options
        switch ($engine) {
            case 'ReplacingMergeTree':
                if (isset($options['version_column'])) {
                    $engineClause .= "({$this->wrap($options['version_column'])})";
                } else {
                    $engineClause .= '()';
                }
                break;

            case 'SummingMergeTree':
                if (isset($options['columns']) && ! empty($options['columns'])) {
                    $columns = implode(', ', array_map([$this, 'wrap'], $options['columns']));
                    $engineClause .= "({$columns})";
                } else {
                    $engineClause .= '()';
                }
                break;

            case 'CollapsingMergeTree':
                if (isset($options['sign_column'])) {
                    $engineClause .= "({$this->wrap($options['sign_column'])})";
                }
                break;

            case 'MergeTree':
                $engineClause .= '()';
                break;
        }

        return $engineClause;
    }

    /**
     * Compile order by clause.
     *
     * @param  mixed  $blueprint
     * @param  mixed  $command
     * @return string
     */
    protected function compileOrderBy($blueprint, $command): string
    {
        $columns = is_array($command->columns) ? $command->columns : [$command->columns];
        $columnList = implode(', ', array_map([$this, 'wrap'], $columns));

        return " ORDER BY ({$columnList})";
    }

    /**
     * Compile partition by clause.
     *
     * @param  mixed  $blueprint
     * @param  mixed  $command
     * @return string
     */
    protected function compilePartitionBy($blueprint, $command): string
    {
        $columns = is_array($command->columns) ? $command->columns : [$command->columns];
        $columnList = implode(', ', array_map([$this, 'wrap'], $columns));

        return " PARTITION BY ({$columnList})";
    }

    /**
     * Compile primary key clause.
     *
     * @param  mixed  $blueprint
     * @param  mixed  $command
     * @return string
     */
    protected function compilePrimaryKey($blueprint, $command): string
    {
        $columns = is_array($command->columns) ? $command->columns : [$command->columns];
        $columnList = implode(', ', array_map([$this, 'wrap'], $columns));

        return " PRIMARY KEY ({$columnList})";
    }

    /**
     * Compile a drop table command.
     *
     * @param  mixed  $blueprint  The table blueprint
     * @param  mixed  $command  The drop command
     * @return string
     */
    public function compileDrop($blueprint, $command): string
    {
        return 'DROP TABLE '.$this->wrapTable($blueprint);
    }

    /**
     * Compile a drop table if exists command.
     *
     * @param  mixed  $blueprint  The table blueprint
     * @param  mixed  $command  The drop if exists command
     * @return string
     */
    public function compileDropIfExists($blueprint, $command): string
    {
        return 'DROP TABLE IF EXISTS '.$this->wrapTable($blueprint);
    }

    /**
     * Compile an add column command.
     *
     * @param  mixed  $blueprint  The table blueprint
     * @param  mixed  $command  The add column command
     * @return string
     */
    public function compileAdd($blueprint, $command): string
    {
        $table = $this->wrapTable($blueprint);
        $columns = $this->prefixArray('ADD COLUMN', $this->getColumns($blueprint));

        return 'ALTER TABLE '.$table.' '.implode(', ', $columns);
    }

    /**
     * Compile a drop column command.
     *
     * @param  mixed  $blueprint  The table blueprint
     * @param  mixed  $command  The drop column command
     * @return string
     */
    public function compileDropColumn($blueprint, $command): string
    {
        $table = $this->wrapTable($blueprint);
        $columns = $this->prefixArray('DROP COLUMN', $this->wrapArray($command->columns));

        return 'ALTER TABLE '.$table.' '.implode(', ', $columns);
    }

    /**
     * Get the SQL for a string column type.
     *
     * @param  mixed  $column  The column definition
     * @return string
     */
    protected function typeString($column): string
    {
        return 'String';
    }

    /**
     * Get the SQL for an integer column type.
     *
     * @param  mixed  $column  The column definition
     * @return string
     */
    protected function typeInteger($column): string
    {
        return 'Int32';
    }

    /**
     * Get the SQL for a big integer column type.
     *
     * @param  mixed  $column  The column definition
     * @return string
     */
    protected function typeBigInteger($column): string
    {
        return 'Int64';
    }

    /**
     * Get the SQL for a float column type.
     *
     * @param  mixed  $column  The column definition
     * @return string
     */
    protected function typeFloat($column): string
    {
        return 'Float32';
    }

    /**
     * Get the SQL for a double column type.
     *
     * @param  mixed  $column  The column definition
     * @return string
     */
    protected function typeDouble($column): string
    {
        return 'Float64';
    }

    /**
     * Get the SQL for a boolean column type.
     *
     * @param  mixed  $column  The column definition
     * @return string
     */
    protected function typeBoolean($column): string
    {
        return 'UInt8';
    }

    /**
     * Get the SQL for a date column type.
     *
     * @param  mixed  $column  The column definition
     * @return string
     */
    protected function typeDate($column): string
    {
        return 'Date';
    }

    /**
     * Get the SQL for a datetime column type.
     *
     * @param  mixed  $column  The column definition
     * @return string
     */
    protected function typeDateTime($column): string
    {
        return 'DateTime';
    }

    /**
     * Get the SQL for a timestamp column type.
     *
     * @param  mixed  $column  The column definition
     * @return string
     */
    protected function typeTimestamp($column): string
    {
        return 'DateTime';
    }

    /**
     * Check if a command exists in the blueprint.
     *
     * @param  mixed  $blueprint
     * @param  string  $name
     * @return bool
     */
    protected function hasCommand($blueprint, $name): bool
    {
        return $this->getCommandByName($blueprint, $name) !== null;
    }

    /**
     * Get a command by name from the blueprint.
     *
     * @param  mixed  $blueprint
     * @param  mixed  $name
     * @return mixed|null
     */
    protected function getCommandByName($blueprint, $name)
    {
        $commands = array_filter($blueprint->getCommands(), function ($value) use ($name) {
            return $value->name === $name;
        });

        return reset($commands) ?: null;
    }

    /**
     * Get the SQL for a UUID column type.
     *
     * @param  mixed  $column
     * @return string
     */
    protected function typeUuid($column): string
    {
        return 'UUID';
    }

    /**
     * Get the SQL for an array column type.
     *
     * @param  mixed  $column
     * @return string
     */
    protected function typeArray($column): string
    {
        return "Array({$column->type})";
    }
}
