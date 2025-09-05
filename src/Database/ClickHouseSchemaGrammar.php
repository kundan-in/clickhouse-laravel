<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Schema\Grammars\Grammar;

/**
 * ClickHouse Schema Grammar
 *
 * This class extends Laravel's Schema Grammar class to provide ClickHouse-specific
 * DDL (Data Definition Language) operations and schema management.
 *
 * @package KundanIn\ClickHouseLaravel\Database
 */
class ClickHouseSchemaGrammar extends Grammar
{
    /**
     * Compile a create table command.
     *
     * @param mixed $blueprint The table blueprint
     * @param mixed $command The create command
     * @return string
     */
    public function compileCreate($blueprint, $command): string
    {
        $table = $this->wrapTable($blueprint);
        $columns = implode(', ', $this->getColumns($blueprint));
        
        // ClickHouse requires an ENGINE specification
        $engine = $blueprint->engine ?? 'MergeTree()';
        
        return "CREATE TABLE {$table} ({$columns}) ENGINE = {$engine}";
    }

    /**
     * Compile a drop table command.
     *
     * @param mixed $blueprint The table blueprint
     * @param mixed $command The drop command
     * @return string
     */
    public function compileDrop($blueprint, $command): string
    {
        return 'DROP TABLE ' . $this->wrapTable($blueprint);
    }

    /**
     * Compile a drop table if exists command.
     *
     * @param mixed $blueprint The table blueprint
     * @param mixed $command The drop if exists command
     * @return string
     */
    public function compileDropIfExists($blueprint, $command): string
    {
        return 'DROP TABLE IF EXISTS ' . $this->wrapTable($blueprint);
    }

    /**
     * Compile an add column command.
     *
     * @param mixed $blueprint The table blueprint
     * @param mixed $command The add column command
     * @return string
     */
    public function compileAdd($blueprint, $command): string
    {
        $table = $this->wrapTable($blueprint);
        $columns = $this->prefixArray('ADD COLUMN', $this->getColumns($blueprint));
        
        return 'ALTER TABLE ' . $table . ' ' . implode(', ', $columns);
    }

    /**
     * Compile a drop column command.
     *
     * @param mixed $blueprint The table blueprint
     * @param mixed $command The drop column command
     * @return string
     */
    public function compileDropColumn($blueprint, $command): string
    {
        $table = $this->wrapTable($blueprint);
        $columns = $this->prefixArray('DROP COLUMN', $this->wrapArray($command->columns));
        
        return 'ALTER TABLE ' . $table . ' ' . implode(', ', $columns);
    }

    /**
     * Get the SQL for a string column type.
     *
     * @param mixed $column The column definition
     * @return string
     */
    protected function typeString($column): string
    {
        return 'String';
    }

    /**
     * Get the SQL for an integer column type.
     *
     * @param mixed $column The column definition
     * @return string
     */
    protected function typeInteger($column): string
    {
        return 'Int32';
    }

    /**
     * Get the SQL for a big integer column type.
     *
     * @param mixed $column The column definition
     * @return string
     */
    protected function typeBigInteger($column): string
    {
        return 'Int64';
    }

    /**
     * Get the SQL for a float column type.
     *
     * @param mixed $column The column definition
     * @return string
     */
    protected function typeFloat($column): string
    {
        return 'Float32';
    }

    /**
     * Get the SQL for a double column type.
     *
     * @param mixed $column The column definition
     * @return string
     */
    protected function typeDouble($column): string
    {
        return 'Float64';
    }

    /**
     * Get the SQL for a boolean column type.
     *
     * @param mixed $column The column definition
     * @return string
     */
    protected function typeBoolean($column): string
    {
        return 'UInt8';
    }

    /**
     * Get the SQL for a date column type.
     *
     * @param mixed $column The column definition
     * @return string
     */
    protected function typeDate($column): string
    {
        return 'Date';
    }

    /**
     * Get the SQL for a datetime column type.
     *
     * @param mixed $column The column definition
     * @return string
     */
    protected function typeDateTime($column): string
    {
        return 'DateTime';
    }

    /**
     * Get the SQL for a timestamp column type.
     *
     * @param mixed $column The column definition
     * @return string
     */
    protected function typeTimestamp($column): string
    {
        return 'DateTime';
    }
}