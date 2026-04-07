<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Grammars\Grammar;
use Illuminate\Support\Fluent;

/**
 * ClickHouse schema grammar for Laravel.
 *
 * Compiles schema operations (CREATE TABLE, ALTER TABLE, DROP TABLE) into
 * ClickHouse-compatible DDL. Supports all MergeTree engine variants,
 * ClickHouse-specific column types, and clauses like SAMPLE BY, TTL, SETTINGS.
 */
class ClickHouseSchemaGrammar extends Grammar
{
    /**
     * The possible column modifiers.
     *
     * @var string[]
     */
    protected $modifiers = ['Default'];

    // -----------------------------------------------------------------
    // Schema inspection
    // -----------------------------------------------------------------

    /**
     * Compile the query to determine if a table exists.
     *
     * Called by SchemaBuilder::hasTable() with bindings [$database, $table].
     * Also called by parent with ($schema, $table) args — we accept both.
     *
     * @param  string  $schema
     * @param  string  $table
     * @return string
     */
    public function compileTableExists($schema = '', $table = '')
    {
        return "SELECT 1 FROM system.tables WHERE database = ? AND name = ?";
    }

    /**
     * Compile the query to get the column listing for a table.
     *
     * Called by SchemaBuilder with bindings [$database, $table].
     *
     * @return string
     */
    public function compileColumnListing()
    {
        return "SELECT name FROM system.columns WHERE database = ? AND table = ? ORDER BY position";
    }

    /**
     * Compile the query to get a single column's data type.
     *
     * Called by SchemaBuilder with bindings [$database, $table, $column].
     *
     * @return string
     */
    public function compileColumnType()
    {
        return "SELECT type FROM system.columns WHERE database = ? AND table = ? AND name = ?";
    }

    /**
     * Compile the query to get full column information for a table.
     *
     * Returns name, type, default expression, and comment for all columns
     * in the given schema and table, ordered by position.
     *
     * @param  string  $schema
     * @param  string  $table
     * @return string
     */
    public function compileColumns($schema, $table)
    {
        return sprintf(
            "SELECT name, type, default_expression AS \"default\", comment "
            ."FROM system.columns WHERE database = '%s' AND table = '%s' ORDER BY position",
            $schema,
            $table
        );
    }

    // -----------------------------------------------------------------
    // CREATE TABLE
    // -----------------------------------------------------------------

    /**
     * Compile a CREATE TABLE statement.
     *
     * Assembles the column definitions, engine, ORDER BY, PARTITION BY,
     * PRIMARY KEY, SAMPLE BY, TTL, and SETTINGS clauses.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    public function compileCreate($blueprint, $command): string
    {
        $sql = $this->compileCreateTable($blueprint);

        $sql .= $this->compileEngine($blueprint);

        if ($this->hasCommand($blueprint, 'orderBy')) {
            $sql .= $this->compileOrderBy($blueprint, $this->getCommandByName($blueprint, 'orderBy'));
        }

        if ($this->hasCommand($blueprint, 'partitionBy')) {
            $sql .= $this->compilePartitionBy($blueprint, $this->getCommandByName($blueprint, 'partitionBy'));
        }

        if ($this->hasCommand($blueprint, 'primaryKey')) {
            $sql .= $this->compilePrimaryKey($blueprint, $this->getCommandByName($blueprint, 'primaryKey'));
        }

        if ($this->hasCommand($blueprint, 'sampleBy')) {
            $sql .= $this->compileSampleBy($blueprint, $this->getCommandByName($blueprint, 'sampleBy'));
        }

        if ($this->hasCommand($blueprint, 'ttl')) {
            $sql .= $this->compileTtl($blueprint, $this->getCommandByName($blueprint, 'ttl'));
        }

        if ($this->hasCommand($blueprint, 'settings')) {
            $sql .= $this->compileSettingsClause($blueprint, $this->getCommandByName($blueprint, 'settings'));
        }

        return $sql;
    }

    /**
     * Compile the basic CREATE TABLE clause with column definitions.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
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
     * Compile the ENGINE clause with engine-specific options.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @return string
     */
    protected function compileEngine($blueprint): string
    {
        $engine = $blueprint->engine ?? 'MergeTree';
        $options = $blueprint->engineOptions ?? [];

        $engineClause = " ENGINE = {$engine}";

        switch ($engine) {
            case 'ReplacingMergeTree':
                $engineClause .= isset($options['version_column'])
                    ? "({$this->wrap($options['version_column'])})"
                    : '()';
                break;

            case 'SummingMergeTree':
                if (! empty($options['columns'])) {
                    $engineClause .= '('.implode(', ', array_map([$this, 'wrap'], $options['columns'])).')';
                } else {
                    $engineClause .= '()';
                }
                break;

            case 'CollapsingMergeTree':
                if (isset($options['sign_column'])) {
                    $engineClause .= "({$this->wrap($options['sign_column'])})";
                }
                break;

            case 'VersionedCollapsingMergeTree':
                if (isset($options['sign_column'], $options['version_column'])) {
                    $engineClause .= "({$this->wrap($options['sign_column'])}, {$this->wrap($options['version_column'])})";
                }
                break;

            case 'AggregatingMergeTree':
            case 'MergeTree':
                $engineClause .= '()';
                break;

            case 'GraphiteMergeTree':
                if (isset($options['config_section'])) {
                    $engineClause .= "('{$options['config_section']}')";
                }
                break;

            default:
                break;
        }

        return $engineClause;
    }

    /**
     * Compile the ORDER BY clause for a MergeTree table.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    protected function compileOrderBy($blueprint, $command): string
    {
        $columns = is_array($command->columns) ? $command->columns : [$command->columns];

        return ' ORDER BY ('.implode(', ', array_map([$this, 'wrap'], $columns)).')';
    }

    /**
     * Compile the PARTITION BY clause.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    protected function compilePartitionBy($blueprint, $command): string
    {
        $columns = is_array($command->columns) ? $command->columns : [$command->columns];

        return ' PARTITION BY ('.implode(', ', array_map([$this, 'wrap'], $columns)).')';
    }

    /**
     * Compile the PRIMARY KEY clause.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    protected function compilePrimaryKey($blueprint, $command): string
    {
        $columns = is_array($command->columns) ? $command->columns : [$command->columns];

        return ' PRIMARY KEY ('.implode(', ', array_map([$this, 'wrap'], $columns)).')';
    }

    /**
     * Compile the SAMPLE BY clause.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    protected function compileSampleBy($blueprint, $command): string
    {
        return ' SAMPLE BY '.$this->wrap($command->column);
    }

    /**
     * Compile the TTL clause for automatic data expiration.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    protected function compileTtl($blueprint, $command): string
    {
        return ' TTL '.$command->expression;
    }

    /**
     * Compile the SETTINGS clause for table-level settings.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    protected function compileSettingsClause($blueprint, $command): string
    {
        $settings = [];

        foreach ($command->settings as $key => $value) {
            $settings[] = "{$key} = {$value}";
        }

        return ' SETTINGS '.implode(', ', $settings);
    }

    // -----------------------------------------------------------------
    // Materialized views
    // -----------------------------------------------------------------

    /**
     * Compile a CREATE MATERIALIZED VIEW statement.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    public function compileCreateMaterializedView($blueprint, $command): string
    {
        $sql = "CREATE MATERIALIZED VIEW {$this->wrap($command->viewName)}";

        if (! empty($command->toTable)) {
            $sql .= " TO {$this->wrapTable($command->toTable)}";
        }

        $sql .= " AS {$command->query}";

        return $sql;
    }

    /**
     * Compile a DROP MATERIALIZED VIEW statement.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    public function compileDropMaterializedView($blueprint, $command): string
    {
        return "DROP VIEW IF EXISTS {$this->wrap($command->viewName)}";
    }

    // -----------------------------------------------------------------
    // ALTER TABLE
    // -----------------------------------------------------------------

    /**
     * Compile a DROP TABLE statement.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    public function compileDrop($blueprint, $command): string
    {
        return 'DROP TABLE '.$this->wrapTable($blueprint);
    }

    /**
     * Compile a DROP TABLE IF EXISTS statement.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    public function compileDropIfExists($blueprint, $command): string
    {
        return 'DROP TABLE IF EXISTS '.$this->wrapTable($blueprint);
    }

    /**
     * Compile an ALTER TABLE ADD COLUMN statement.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    public function compileAdd($blueprint, $command): string
    {
        $columns = $this->prefixArray('ADD COLUMN', $this->getColumns($blueprint));

        return 'ALTER TABLE '.$this->wrapTable($blueprint).' '.implode(', ', $columns);
    }

    /**
     * Compile an ALTER TABLE DROP COLUMN statement.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $command
     * @return string
     */
    public function compileDropColumn($blueprint, $command): string
    {
        $columns = $this->prefixArray('DROP COLUMN', $this->wrapArray($command->columns));

        return 'ALTER TABLE '.$this->wrapTable($blueprint).' '.implode(', ', $columns);
    }

    // -----------------------------------------------------------------
    // Standard Laravel column types mapped to ClickHouse equivalents
    // -----------------------------------------------------------------

    /**
     * Get the ClickHouse type for a string column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeString($column): string
    {
        return 'String';
    }

    /**
     * Get the ClickHouse type for a text column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeText($column): string
    {
        return 'String';
    }

    /**
     * Get the ClickHouse type for a medium text column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeMediumText($column): string
    {
        return 'String';
    }

    /**
     * Get the ClickHouse type for a long text column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeLongText($column): string
    {
        return 'String';
    }

    /**
     * Get the ClickHouse type for a char column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeChar($column): string
    {
        return isset($column->length) ? "FixedString({$column->length})" : 'String';
    }

    /**
     * Get the ClickHouse type for an integer column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeInteger($column): string
    {
        return 'Int32';
    }

    /**
     * Get the ClickHouse type for a small integer column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeSmallInteger($column): string
    {
        return 'Int16';
    }

    /**
     * Get the ClickHouse type for a tiny integer column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeTinyInteger($column): string
    {
        return 'Int8';
    }

    /**
     * Get the ClickHouse type for a medium integer column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeMediumInteger($column): string
    {
        return 'Int32';
    }

    /**
     * Get the ClickHouse type for a big integer column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeBigInteger($column): string
    {
        return 'Int64';
    }

    /**
     * Get the ClickHouse type for an unsigned integer column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeUnsignedInteger($column): string
    {
        return 'UInt32';
    }

    /**
     * Get the ClickHouse type for an unsigned small integer column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeUnsignedSmallInteger($column): string
    {
        return 'UInt16';
    }

    /**
     * Get the ClickHouse type for an unsigned tiny integer column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeUnsignedTinyInteger($column): string
    {
        return 'UInt8';
    }

    /**
     * Get the ClickHouse type for an unsigned medium integer column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeUnsignedMediumInteger($column): string
    {
        return 'UInt32';
    }

    /**
     * Get the ClickHouse type for an unsigned big integer column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeUnsignedBigInteger($column): string
    {
        return 'UInt64';
    }

    /**
     * Get the ClickHouse type for a float column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeFloat($column): string
    {
        return 'Float32';
    }

    /**
     * Get the ClickHouse type for a double column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeDouble($column): string
    {
        return 'Float64';
    }

    /**
     * Get the ClickHouse type for a decimal column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeDecimal($column): string
    {
        $precision = $column->precision ?? $column->total ?? 8;
        $scale = $column->scale ?? $column->places ?? 2;

        return "Decimal({$precision}, {$scale})";
    }

    /**
     * Get the ClickHouse type for a boolean column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeBoolean($column): string
    {
        return 'UInt8';
    }

    /**
     * Get the ClickHouse type for a date column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeDate($column): string
    {
        return 'Date';
    }

    /**
     * Get the ClickHouse type for a datetime column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeDateTime($column): string
    {
        if (isset($column->timezone)) {
            return "DateTime('{$column->timezone}')";
        }

        return 'DateTime';
    }

    /**
     * Get the ClickHouse type for a timestamp column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeTimestamp($column): string
    {
        return $this->typeDateTime($column);
    }

    /**
     * Get the ClickHouse type for a JSON column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeJson($column): string
    {
        return 'String';
    }

    /**
     * Get the ClickHouse type for a JSONB column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeJsonb($column): string
    {
        return 'String';
    }

    /**
     * Get the ClickHouse type for a binary column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeBinary($column): string
    {
        return 'String';
    }

    /**
     * Get the ClickHouse type for a UUID column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeUuid($column): string
    {
        return 'UUID';
    }

    /**
     * Get the ClickHouse type for an IP address column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeIpAddress($column): string
    {
        return 'IPv4';
    }

    /**
     * Get the ClickHouse type for a MAC address column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeMacAddress($column): string
    {
        return 'String';
    }

    /**
     * Get the ClickHouse type for an enum column.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeEnum($column): string
    {
        if (! empty($column->allowed)) {
            $values = implode(', ', array_map(function ($value, $index) {
                return "'{$value}' = ".($index + 1);
            }, $column->allowed, array_keys($column->allowed)));

            return "Enum8({$values})";
        }

        return 'String';
    }

    // -----------------------------------------------------------------
    // ClickHouse-specific column types
    // -----------------------------------------------------------------

    /**
     * Get the ClickHouse Int8 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeInt8($column): string
    {
        return 'Int8';
    }

    /**
     * Get the ClickHouse Int16 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeInt16($column): string
    {
        return 'Int16';
    }

    /**
     * Get the ClickHouse Int32 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeInt32($column): string
    {
        return 'Int32';
    }

    /**
     * Get the ClickHouse Int64 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeInt64($column): string
    {
        return 'Int64';
    }

    /**
     * Get the ClickHouse UInt8 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeUint8($column): string
    {
        return 'UInt8';
    }

    /**
     * Get the ClickHouse UInt16 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeUint16($column): string
    {
        return 'UInt16';
    }

    /**
     * Get the ClickHouse UInt32 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeUint32($column): string
    {
        return 'UInt32';
    }

    /**
     * Get the ClickHouse UInt64 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeUint64($column): string
    {
        return 'UInt64';
    }

    /**
     * Get the ClickHouse Float32 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeFloat32($column): string
    {
        return 'Float32';
    }

    /**
     * Get the ClickHouse Float64 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeFloat64($column): string
    {
        return 'Float64';
    }

    /**
     * Get the ClickHouse FixedString type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeFixedString($column): string
    {
        return "FixedString({$column->length})";
    }

    /**
     * Get the ClickHouse DateTime64 type with sub-second precision.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeDatetime64($column): string
    {
        $precision = $column->precision ?? 3;

        if (isset($column->timezone)) {
            return "DateTime64({$precision}, '{$column->timezone}')";
        }

        return "DateTime64({$precision})";
    }

    /**
     * Get the ClickHouse Array type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeArray($column): string
    {
        $elementType = $column->arrayType ?? 'String';

        return "Array({$elementType})";
    }

    /**
     * Get the ClickHouse Tuple type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeTuple($column): string
    {
        $types = implode(', ', $column->types ?? ['String']);

        return "Tuple({$types})";
    }

    /**
     * Get the ClickHouse Map type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeMap($column): string
    {
        $keyType = $column->keyType ?? 'String';
        $valueType = $column->valueType ?? 'String';

        return "Map({$keyType}, {$valueType})";
    }

    /**
     * Get the ClickHouse Nested type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeNested($column): string
    {
        $fields = [];
        foreach ($column->structure ?? [] as $name => $type) {
            $fields[] = "{$name} {$type}";
        }

        return 'Nested('.implode(', ', $fields).')';
    }

    /**
     * Get the ClickHouse Enum8 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeEnum8($column): string
    {
        $items = $column->values ?? $column->allowed ?? [];
        $values = implode(', ', array_map(function ($value, $index) {
            return "'{$value}' = ".($index + 1);
        }, $items, array_keys($items)));

        return "Enum8({$values})";
    }

    /**
     * Get the ClickHouse Enum16 type.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeEnum16($column): string
    {
        $items = $column->values ?? $column->allowed ?? [];
        $values = implode(', ', array_map(function ($value, $index) {
            return "'{$value}' = ".($index + 1);
        }, $items, array_keys($items)));

        return "Enum16({$values})";
    }

    /**
     * Get the ClickHouse LowCardinality type for dictionary-encoded columns.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeLowCardinality($column): string
    {
        $innerType = $column->innerType ?? 'String';

        return "LowCardinality({$innerType})";
    }

    /**
     * Get the ClickHouse Nullable type wrapper.
     *
     * @param  \Illuminate\Support\Fluent  $column
     * @return string
     */
    protected function typeNullableColumn($column): string
    {
        $innerType = $column->innerType ?? 'String';

        return "Nullable({$innerType})";
    }

    // -----------------------------------------------------------------
    // Column modifiers
    // -----------------------------------------------------------------

    /**
     * Compile a DEFAULT column modifier.
     *
     * @param  \Illuminate\Database\Schema\Blueprint  $blueprint
     * @param  \Illuminate\Support\Fluent  $column
     * @return string|null
     */
    protected function modifyDefault(Blueprint $blueprint, Fluent $column): ?string
    {
        if (isset($column->default)) {
            $value = $column->default;

            if (is_bool($value)) {
                return ' DEFAULT '.($value ? '1' : '0');
            }

            if (is_int($value) || is_float($value)) {
                return ' DEFAULT '.$value;
            }

            if (is_string($value) && preg_match('/^[a-zA-Z_]+\(.*\)$/', $value)) {
                return ' DEFAULT '.$value;
            }

            return " DEFAULT '{$value}'";
        }

        return null;
    }
}
