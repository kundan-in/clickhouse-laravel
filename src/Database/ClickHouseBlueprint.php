<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Schema\Blueprint;

/**
 * ClickHouse Blueprint
 *
 * This class extends Laravel's Blueprint to provide ClickHouse-specific
 * column types and table operations.
 */
class ClickHouseBlueprint extends Blueprint
{
    /**
     * The ClickHouse engine for the table.
     *
     * @var string
     */
    public $engine = 'MergeTree';

    /**
     * The ClickHouse engine options.
     *
     * @var array
     */
    public $engineOptions = [];

    /**
     * Set the ClickHouse engine for the table.
     *
     * @param  string  $engine
     * @param  array  $options
     * @return \Illuminate\Support\Fluent
     */
    public function engine($engine, array $options = [])
    {
        $this->engine = $engine;
        $this->engineOptions = $options;

        return $this->addCommand('engine', compact('engine', 'options'));
    }

    /**
     * Set the table to use MergeTree engine.
     *
     * @param  array  $options
     * @return \Illuminate\Support\Fluent
     */
    public function mergeTree(array $options = [])
    {
        return $this->engine('MergeTree', $options);
    }

    /**
     * Set the table to use ReplacingMergeTree engine.
     *
     * @param  string|null  $versionColumn
     * @param  array  $options
     * @return \Illuminate\Support\Fluent
     */
    public function replacingMergeTree($versionColumn = null, array $options = [])
    {
        if ($versionColumn) {
            $options['version_column'] = $versionColumn;
        }

        return $this->engine('ReplacingMergeTree', $options);
    }

    /**
     * Set ORDER BY clause for MergeTree engines.
     *
     * @param  string|array  $columns
     * @return \Illuminate\Support\Fluent
     */
    public function orderBy($columns)
    {
        return $this->addCommand('orderBy', compact('columns'));
    }

    /**
     * Set PARTITION BY clause for MergeTree engines.
     *
     * @param  string|array  $columns
     * @return \Illuminate\Support\Fluent
     */
    public function partitionBy($columns)
    {
        return $this->addCommand('partitionBy', compact('columns'));
    }

    /**
     * Set PRIMARY KEY clause.
     *
     * @param  string|array  $columns
     * @return \Illuminate\Support\Fluent
     */
    public function primaryKey($columns)
    {
        return $this->addCommand('primaryKey', compact('columns'));
    }

    /**
     * Set SAMPLE BY clause.
     *
     * @param  string  $column
     * @return \Illuminate\Support\Fluent
     */
    public function sampleBy($column)
    {
        return $this->addCommand('sampleBy', compact('column'));
    }

    /**
     * Set TTL clause.
     *
     * @param  string  $expression
     * @return \Illuminate\Support\Fluent
     */
    public function ttl($expression)
    {
        return $this->addCommand('ttl', compact('expression'));
    }

    /**
     * Set SETTINGS clause.
     *
     * @param  array  $settings
     * @return \Illuminate\Support\Fluent
     */
    public function settings(array $settings)
    {
        return $this->addCommand('settings', compact('settings'));
    }

    /**
     * Create an Int8 column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function int8($column)
    {
        return $this->addColumn('int8', $column);
    }

    /**
     * Create an Int16 column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function int16($column)
    {
        return $this->addColumn('int16', $column);
    }

    /**
     * Create an Int32 column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function int32($column)
    {
        return $this->addColumn('int32', $column);
    }

    /**
     * Create an Int64 column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function int64($column)
    {
        return $this->addColumn('int64', $column);
    }

    /**
     * Create a UInt8 column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function uint8($column)
    {
        return $this->addColumn('uint8', $column);
    }

    /**
     * Create a UInt16 column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function uint16($column)
    {
        return $this->addColumn('uint16', $column);
    }

    /**
     * Create a UInt32 column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function uint32($column)
    {
        return $this->addColumn('uint32', $column);
    }

    /**
     * Create a UInt64 column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function uint64($column)
    {
        return $this->addColumn('uint64', $column);
    }

    /**
     * Create a Float32 column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function float32($column)
    {
        return $this->addColumn('float32', $column);
    }

    /**
     * Create a Float64 column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function float64($column)
    {
        return $this->addColumn('float64', $column);
    }

    /**
     * Create a Decimal column.
     *
     * @param  string  $column
     * @param  int  $precision
     * @param  int  $scale
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function decimal($column, $precision = 8, $scale = 2)
    {
        return $this->addColumn('decimal', $column, compact('precision', 'scale'));
    }

    /**
     * Create a FixedString column.
     *
     * @param  string  $column
     * @param  int  $length
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function fixedString($column, $length)
    {
        return $this->addColumn('fixedString', $column, compact('length'));
    }

    /**
     * Create a UUID column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function uuid($column = 'uuid')
    {
        return $this->addColumn('uuid', $column);
    }

    /**
     * Create an Array column.
     *
     * @param  string  $column
     * @param  string  $type
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function array($column, $type = 'String')
    {
        return $this->addColumn('array', $column, compact('type'));
    }

    /**
     * Create a Tuple column.
     *
     * @param  string  $column
     * @param  array  $types
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function tuple($column, array $types)
    {
        return $this->addColumn('tuple', $column, compact('types'));
    }

    /**
     * Create a Map column.
     *
     * @param  string  $column
     * @param  string  $keyType
     * @param  string  $valueType
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function map($column, $keyType = 'String', $valueType = 'String')
    {
        return $this->addColumn('map', $column, compact('keyType', 'valueType'));
    }

    /**
     * Create a Nested column.
     *
     * @param  string  $column
     * @param  array  $structure
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function nested($column, array $structure)
    {
        return $this->addColumn('nested', $column, compact('structure'));
    }

    /**
     * Create an Enum8 column.
     *
     * @param  string  $column
     * @param  array  $values
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function enum8($column, array $values)
    {
        return $this->addColumn('enum8', $column, compact('values'));
    }

    /**
     * Create an Enum16 column.
     *
     * @param  string  $column
     * @param  array  $values
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function enum16($column, array $values)
    {
        return $this->addColumn('enum16', $column, compact('values'));
    }

    /**
     * Create a DateTime column.
     *
     * @param  string  $column
     * @param  string|null  $timezone
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function dateTime($column = 'created_at', $timezone = null)
    {
        return $this->addColumn('datetime', $column, compact('timezone'));
    }

    /**
     * Create a DateTime64 column.
     *
     * @param  string  $column
     * @param  int  $precision
     * @param  string|null  $timezone
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function dateTime64($column = 'created_at', $precision = 3, $timezone = null)
    {
        return $this->addColumn('datetime64', $column, compact('precision', 'timezone'));
    }

    /**
     * Create a Date column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function date($column)
    {
        return $this->addColumn('date', $column);
    }

    /**
     * Create a LowCardinality column.
     *
     * @param  string  $column
     * @param  string  $type
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function lowCardinality($column, $type = 'String')
    {
        return $this->addColumn('lowCardinality', $column, compact('type'));
    }

    /**
     * Create a Nullable column.
     *
     * @param  string  $column
     * @param  string  $type
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function nullable($column, $type)
    {
        return $this->addColumn('nullable', $column, compact('type'));
    }

    /**
     * Create a materialized view.
     *
     * @param  string  $query
     * @param  string  $toTable
     * @param  array  $options
     * @return \Illuminate\Support\Fluent
     */
    public function createMaterializedView($query, $toTable, array $options = [])
    {
        return $this->addCommand('createMaterializedView', compact('query', 'toTable', 'options'));
    }

    /**
     * Drop a materialized view.
     *
     * @return \Illuminate\Support\Fluent
     */
    public function dropMaterializedView()
    {
        return $this->addCommand('dropMaterializedView');
    }

    /**
     * Create ClickHouse-style timestamps.
     *
     * @param  int  $precision
     * @return void
     */
    public function timestamps($precision = 3)
    {
        $this->dateTime64('created_at', $precision)->default('now()');
        $this->dateTime64('updated_at', $precision)->default('now()');
    }

    /**
     * Add soft deletes column (ClickHouse doesn't support real deletes).
     *
     * @param  string  $column
     * @param  int  $precision
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function softDeletes($column = 'deleted_at', $precision = 3)
    {
        return $this->dateTime64($column, $precision)->nullable();
    }

    /**
     * Add a soft delete flag column.
     *
     * @param  string  $column
     * @return \Illuminate\Database\Schema\ColumnDefinition
     */
    public function softDeletesFlag($column = 'is_deleted')
    {
        return $this->uint8($column)->default(0);
    }
}
