<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Closure;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Builder;

/**
 * ClickHouse Schema Builder
 *
 * This class extends Laravel's Schema Builder to provide ClickHouse-specific
 * schema building capabilities and handles ClickHouse limitations.
 */
class ClickHouseSchemaBuilder extends Builder
{
    /**
     * Create a new table on the schema with ClickHouse engine support.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @param  string  $engine
     * @param  array  $engineOptions
     * @return void
     */
    public function create($table, ?Closure $callback = null, $engine = 'MergeTree', array $engineOptions = [])
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback, $engine, $engineOptions) {
            $blueprint->create();
            $blueprint->engine($engine, $engineOptions);

            if ($callback) {
                $callback($blueprint);
            }
        }));
    }

    /**
     * Create a new MergeTree table on the schema.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @param  array  $options
     * @return void
     */
    public function createMergeTree($table, Closure $callback, array $options = [])
    {
        $this->create($table, $callback, 'MergeTree', $options);
    }

    /**
     * Create a new ReplacingMergeTree table on the schema.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @param  string|null  $versionColumn
     * @param  array  $options
     * @return void
     */
    public function createReplacingMergeTree($table, Closure $callback, $versionColumn = null, array $options = [])
    {
        if ($versionColumn) {
            $options['version_column'] = $versionColumn;
        }

        $this->create($table, $callback, 'ReplacingMergeTree', $options);
    }

    /**
     * Create a new SummingMergeTree table on the schema.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @param  array  $columns
     * @param  array  $options
     * @return void
     */
    public function createSummingMergeTree($table, Closure $callback, array $columns = [], array $options = [])
    {
        if (! empty($columns)) {
            $options['columns'] = $columns;
        }

        $this->create($table, $callback, 'SummingMergeTree', $options);
    }

    /**
     * Create a new AggregatingMergeTree table on the schema.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @param  array  $options
     * @return void
     */
    public function createAggregatingMergeTree($table, Closure $callback, array $options = [])
    {
        $this->create($table, $callback, 'AggregatingMergeTree', $options);
    }

    /**
     * Create a new CollapsingMergeTree table on the schema.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @param  string  $signColumn
     * @param  array  $options
     * @return void
     */
    public function createCollapsingMergeTree($table, Closure $callback, $signColumn, array $options = [])
    {
        $options['sign_column'] = $signColumn;

        $this->create($table, $callback, 'CollapsingMergeTree', $options);
    }

    /**
     * Create a new VersionedCollapsingMergeTree table on the schema.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @param  string  $signColumn
     * @param  string  $versionColumn
     * @param  array  $options
     * @return void
     */
    public function createVersionedCollapsingMergeTree($table, Closure $callback, $signColumn, $versionColumn, array $options = [])
    {
        $options['sign_column'] = $signColumn;
        $options['version_column'] = $versionColumn;

        $this->create($table, $callback, 'VersionedCollapsingMergeTree', $options);
    }

    /**
     * Create a new GraphiteMergeTree table on the schema.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @param  string  $configSection
     * @param  array  $options
     * @return void
     */
    public function createGraphiteMergeTree($table, Closure $callback, $configSection, array $options = [])
    {
        $options['config_section'] = $configSection;

        $this->create($table, $callback, 'GraphiteMergeTree', $options);
    }

    /**
     * Create a new Log table on the schema.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @return void
     */
    public function createLog($table, Closure $callback)
    {
        $this->create($table, $callback, 'Log');
    }

    /**
     * Create a new TinyLog table on the schema.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @return void
     */
    public function createTinyLog($table, Closure $callback)
    {
        $this->create($table, $callback, 'TinyLog');
    }

    /**
     * Create a new StripeLog table on the schema.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @return void
     */
    public function createStripeLog($table, Closure $callback)
    {
        $this->create($table, $callback, 'StripeLog');
    }

    /**
     * Create a new Memory table on the schema.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @return void
     */
    public function createMemory($table, Closure $callback)
    {
        $this->create($table, $callback, 'Memory');
    }

    /**
     * Modify a table on the schema.
     * Note: ClickHouse has limited ALTER support.
     *
     * @param  string  $table
     * @param  \Closure  $callback
     * @return void
     */
    public function table($table, Closure $callback)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) use ($callback) {
            $callback($blueprint);
        }));
    }

    /**
     * Drop a table from the schema.
     *
     * @param  string  $table
     * @return void
     */
    public function drop($table)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) {
            $blueprint->drop();
        }));
    }

    /**
     * Drop a table from the schema if it exists.
     *
     * @param  string  $table
     * @return void
     */
    public function dropIfExists($table)
    {
        $this->build(tap($this->createBlueprint($table), function ($blueprint) {
            $blueprint->dropIfExists();
        }));
    }

    /**
     * Drop columns from a table schema.
     * Note: ClickHouse has limited column dropping support.
     *
     * @param  string  $table
     * @param  string|array  $columns
     * @return void
     */
    public function dropColumns($table, $columns)
    {
        $this->table($table, function (Blueprint $blueprint) use ($columns) {
            $blueprint->dropColumn($columns);
        });
    }

    /**
     * Rename a table on the schema.
     *
     * @param  string  $from
     * @param  string  $to
     * @return void
     */
    public function rename($from, $to)
    {
        $this->build(tap($this->createBlueprint($from), function ($blueprint) use ($to) {
            $blueprint->rename($to);
        }));
    }

    /**
     * Determine if the given table exists.
     *
     * @param  string  $table
     * @return bool
     */
    public function hasTable($table)
    {
        $table = $this->connection->getTablePrefix().$table;

        $database = $this->connection->getDatabaseName();

        return count($this->connection->select(
            $this->grammar->compileTableExists(), [$database, $table]
        )) > 0;
    }

    /**
     * Determine if the given table has a given column.
     *
     * @param  string  $table
     * @param  string  $column
     * @return bool
     */
    public function hasColumn($table, $column)
    {
        $table = $this->connection->getTablePrefix().$table;

        $database = $this->connection->getDatabaseName();

        return count($this->connection->select(
            $this->grammar->compileColumnListing(), [$database, $table, $column]
        )) > 0;
    }

    /**
     * Get the column listing for a given table.
     *
     * @param  string  $table
     * @return array
     */
    public function getColumnListing($table)
    {
        $table = $this->connection->getTablePrefix().$table;

        $database = $this->connection->getDatabaseName();

        $results = $this->connection->select(
            $this->grammar->compileColumnListing(), [$database, $table]
        );

        return $this->connection->getPostProcessor()->processColumnListing($results);
    }

    /**
     * Get the data type for the given column name.
     *
     * @param  string  $table
     * @param  string  $column
     * @param  bool  $fullDefinition
     * @return string
     */
    public function getColumnType($table, $column, $fullDefinition = false)
    {
        $table = $this->connection->getTablePrefix().$table;

        $database = $this->connection->getDatabaseName();

        $results = $this->connection->select(
            $this->grammar->compileColumnType(), [$database, $table, $column]
        );

        return $results[0]['type'] ?? 'string';
    }

    /**
     * Create a new command set with a Closure.
     *
     * @param  string  $table
     * @param  \Closure|null  $callback
     * @return \Illuminate\Database\Schema\Blueprint
     */
    protected function createBlueprint($table, ?Closure $callback = null)
    {
        $prefix = $this->connection->getConfig('prefix_indexes')
                    ? $this->connection->getConfig('prefix')
                    : '';

        if (isset($this->resolver)) {
            return call_user_func($this->resolver, $table, $callback, $prefix);
        }

        return new ClickHouseBlueprint($table, $callback, $prefix);
    }

    /**
     * Create a materialized view.
     *
     * @param  string  $name
     * @param  string  $query
     * @param  string  $toTable
     * @param  array  $options
     * @return void
     */
    public function createMaterializedView($name, $query, $toTable, array $options = [])
    {
        $this->build(tap($this->createBlueprint($name), function ($blueprint) use ($query, $toTable, $options) {
            $blueprint->createMaterializedView($query, $toTable, $options);
        }));
    }

    /**
     * Drop a materialized view.
     *
     * @param  string  $name
     * @return void
     */
    public function dropMaterializedView($name)
    {
        $this->build(tap($this->createBlueprint($name), function ($blueprint) {
            $blueprint->dropMaterializedView();
        }));
    }
}
