<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use KundanIn\ClickHouseLaravel\Database\ClickHouseBlueprint;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Database\ClickHouseSchemaGrammar;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * Tests for ClickHouse schema grammar compilation.
 */
class ClickHouseSchemaGrammarCompileTest extends TestCase
{
    protected ClickHouseSchemaGrammar $grammar;

    protected ClickHouseConnection $connection;

    protected function setUp(): void
    {
        parent::setUp();

        $this->connection = new ClickHouseConnection([
            'host' => '127.0.0.1',
            'port' => 8123,
            'username' => 'default',
            'password' => '',
            'database' => 'test_db',
        ]);

        $this->grammar = $this->connection->getSchemaBuilder()->getConnection()->getSchemaGrammar();
    }

    // -----------------------------------------------------------------
    // Schema inspection
    // -----------------------------------------------------------------

    public function test_compile_table_exists(): void
    {
        $sql = $this->grammar->compileTableExists('test_db', 'events');

        $this->assertStringContainsString('system.tables', $sql);
        $this->assertStringContainsString('database', $sql);
        $this->assertStringContainsString('name', $sql);
    }

    public function test_compile_column_listing(): void
    {
        $sql = $this->grammar->compileColumnListing();

        $this->assertStringContainsString('system.columns', $sql);
        $this->assertStringContainsString('name', $sql);
    }

    public function test_compile_column_type(): void
    {
        $sql = $this->grammar->compileColumnType();

        $this->assertStringContainsString('system.columns', $sql);
        $this->assertStringContainsString('type', $sql);
    }

    public function test_compile_columns(): void
    {
        $sql = $this->grammar->compileColumns('test_db', 'events');

        $this->assertStringContainsString('system.columns', $sql);
        $this->assertStringContainsString('test_db', $sql);
        $this->assertStringContainsString('events', $sql);
    }

    // -----------------------------------------------------------------
    // CREATE TABLE with engines
    // -----------------------------------------------------------------

    public function test_compile_create_merge_tree(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $blueprint->uint64('id');
        $blueprint->string('name');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('id');

        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('CREATE TABLE', $sql);
        $this->assertStringContainsString('ENGINE = MergeTree()', $sql);
        $this->assertStringContainsString('ORDER BY', $sql);
    }

    public function test_compile_create_replacing_merge_tree(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $blueprint->uint64('id');
        $blueprint->engine('ReplacingMergeTree', ['version_column' => 'ver']);
        $blueprint->orderBy('id');

        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('ENGINE = ReplacingMergeTree("ver")', $sql);
    }

    public function test_compile_create_summing_merge_tree(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $blueprint->uint64('id');
        $blueprint->engine('SummingMergeTree', ['columns' => ['amount']]);
        $blueprint->orderBy('id');

        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('SummingMergeTree("amount")', $sql);
    }

    public function test_compile_create_collapsing_merge_tree(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $blueprint->uint64('id');
        $blueprint->engine('CollapsingMergeTree', ['sign_column' => 'sign']);
        $blueprint->orderBy('id');

        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('CollapsingMergeTree("sign")', $sql);
    }

    public function test_compile_create_with_partition_by(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $blueprint->uint64('id');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('id');
        $blueprint->partitionBy('created_date');

        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('PARTITION BY', $sql);
        $this->assertStringContainsString('"created_date"', $sql);
    }

    public function test_compile_create_with_primary_key(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $blueprint->uint64('id');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('id');
        $blueprint->primaryKey(['id']);

        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('PRIMARY KEY', $sql);
    }

    public function test_compile_create_with_sample_by(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $blueprint->uint64('id');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('id');
        $blueprint->sampleBy('id');

        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('SAMPLE BY', $sql);
    }

    public function test_compile_create_with_ttl(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $blueprint->uint64('id');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('id');
        $blueprint->ttl('created_at + INTERVAL 30 DAY');

        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('TTL created_at + INTERVAL 30 DAY', $sql);
    }

    public function test_compile_create_with_settings(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $blueprint->uint64('id');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('id');
        $blueprint->settings(['index_granularity' => 8192]);

        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('SETTINGS index_granularity = 8192', $sql);
    }

    // -----------------------------------------------------------------
    // Column types
    // -----------------------------------------------------------------

    public function test_type_int8(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 't');
        $blueprint->int8('col');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('col');
        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('"col" Int8', $sql);
    }

    public function test_type_uint64(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 't');
        $blueprint->uint64('col');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('col');
        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('"col" UInt64', $sql);
    }

    public function test_type_float32(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 't');
        $blueprint->float32('col');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('col');
        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('"col" Float32', $sql);
    }

    public function test_type_decimal(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 't');
        $blueprint->decimal('col', 10, 4);
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('col');
        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('Decimal(10, 4)', $sql);
    }

    public function test_type_datetime64(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 't');
        $blueprint->dateTime64('col', 3);
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('col');
        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('DateTime64(3)', $sql);
    }

    public function test_type_uuid(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 't');
        $blueprint->uuid('col');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('col');
        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('"col" UUID', $sql);
    }

    public function test_type_array(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 't');
        $blueprint->array('tags', 'String');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('tags');
        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('Array(String)', $sql);
    }

    public function test_type_low_cardinality(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 't');
        $blueprint->lowCardinality('status', 'String');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('status');
        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('LowCardinality(String)', $sql);
    }

    public function test_type_nullable_column(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 't');
        $blueprint->nullableColumn('notes', 'String');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('notes');
        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('Nullable(String)', $sql);
    }

    public function test_column_default_modifier(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 't');
        $blueprint->uint8('is_active')->default(1);
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('is_active');
        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('DEFAULT 1', $sql);
    }

    public function test_column_default_function(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 't');
        $blueprint->addColumn('datetime', 'created_at')->default('now()');
        $blueprint->engine('MergeTree');
        $blueprint->orderBy('created_at');
        $sql = $this->grammar->compileCreate($blueprint, $blueprint->getCommands()[0]);

        $this->assertStringContainsString('DEFAULT now()', $sql);
    }

    // -----------------------------------------------------------------
    // DDL operations
    // -----------------------------------------------------------------

    public function test_compile_drop(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $sql = $this->grammar->compileDrop($blueprint, $blueprint->getCommands()[0] ?? new \Illuminate\Support\Fluent);

        $this->assertStringContainsString('DROP TABLE', $sql);
    }

    public function test_compile_drop_if_exists(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $sql = $this->grammar->compileDropIfExists($blueprint, new \Illuminate\Support\Fluent);

        $this->assertStringContainsString('DROP TABLE IF EXISTS', $sql);
    }

    public function test_compile_add_column(): void
    {
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $blueprint->string('new_col');
        $sql = $this->grammar->compileAdd($blueprint, new \Illuminate\Support\Fluent);

        $this->assertStringContainsString('ALTER TABLE', $sql);
        $this->assertStringContainsString('ADD COLUMN', $sql);
        $this->assertStringContainsString('"new_col" String', $sql);
    }

    // -----------------------------------------------------------------
    // Materialized views
    // -----------------------------------------------------------------

    public function test_compile_create_materialized_view(): void
    {
        $command = new \Illuminate\Support\Fluent([
            'viewName' => 'events_daily',
            'toTable' => 'events_daily_agg',
            'query' => 'SELECT toDate(created_at) as day, count() as cnt FROM events GROUP BY day',
        ]);

        $blueprint = new ClickHouseBlueprint($this->connection, 'events');
        $sql = $this->grammar->compileCreateMaterializedView($blueprint, $command);

        $this->assertStringContainsString('CREATE MATERIALIZED VIEW', $sql);
        $this->assertStringContainsString('events_daily', $sql);
        $this->assertStringContainsString('TO', $sql);
        $this->assertStringContainsString('events_daily_agg', $sql);
    }

    public function test_compile_drop_materialized_view(): void
    {
        $command = new \Illuminate\Support\Fluent(['viewName' => 'events_daily']);
        $blueprint = new ClickHouseBlueprint($this->connection, 'events');

        $sql = $this->grammar->compileDropMaterializedView($blueprint, $command);

        $this->assertStringContainsString('DROP VIEW IF EXISTS', $sql);
        $this->assertStringContainsString('events_daily', $sql);
    }
}
