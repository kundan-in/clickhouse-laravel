<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Database\ClickHouseQueryBuilder;
use KundanIn\ClickHouseLaravel\Database\ClickHouseQueryGrammar;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * ClickHouse Query Grammar Extended Test
 *
 * Tests the ClickHouse SQL grammar compilation for various query methods.
 */
class ClickHouseQueryGrammarExtendedTest extends TestCase
{
    protected ClickHouseConnection $connection;
    protected ClickHouseQueryBuilder $builder;
    protected ClickHouseQueryGrammar $grammar;

    /**
     * Set up the test environment.
     *
     * @return void
     */
    protected function setUp(): void
    {
        parent::setUp();

        $config = [
            'host' => '127.0.0.1',
            'port' => 8123,
            'username' => 'default',
            'password' => '',
            'database' => 'test_db',
        ];

        $this->connection = new ClickHouseConnection($config);
        $this->builder = $this->connection->query();
        $this->grammar = new ClickHouseQueryGrammar($this->connection);
    }

    /**
     * Test whereDate compilation.
     *
     * @return void
     */
    public function test_where_date_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereDate('created_at', '2025-09-07');
        $sql = $builder->toSql();

        $this->assertStringContainsString('toDate("created_at") = ?', $sql);
        $this->assertContains('2025-09-07', $builder->getBindings());
    }

    /**
     * Test whereDate compilation with operator.
     *
     * @return void
     */
    public function test_where_date_with_operator_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereDate('created_at', '<=', '2025-09-07');
        $sql = $builder->toSql();

        $this->assertStringContainsString('toDate("created_at") <= ?', $sql);
        $this->assertContains('2025-09-07', $builder->getBindings());
    }

    /**
     * Test whereTime compilation.
     *
     * @return void
     */
    public function test_where_time_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereTime('created_at', '>=', '14:30:00');
        $sql = $builder->toSql();

        $this->assertStringContainsString('toTime("created_at") >= ?', $sql);
    }

    /**
     * Test whereDay compilation.
     *
     * @return void
     */
    public function test_where_day_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereDay('created_at', 7);
        $sql = $builder->toSql();

        $this->assertStringContainsString('toDayOfMonth("created_at") = ?', $sql);
    }

    /**
     * Test whereMonth compilation.
     *
     * @return void
     */
    public function test_where_month_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereMonth('created_at', '>', 6);
        $sql = $builder->toSql();

        $this->assertStringContainsString('toMonth("created_at") > ?', $sql);
    }

    /**
     * Test whereYear compilation.
     *
     * @return void
     */
    public function test_where_year_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereYear('created_at', 2025);
        $sql = $builder->toSql();

        $this->assertStringContainsString('toYear("created_at") = ?', $sql);
    }

    /**
     * Test whereBetween compilation.
     *
     * @return void
     */
    public function test_where_between_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereBetween('id', [1, 100]);
        $sql = $builder->toSql();

        $this->assertStringContainsString('"id" BETWEEN ? AND ?', $sql);
        $this->assertEquals([1, 100], $builder->getBindings()['where'][0]);
    }

    /**
     * Test whereNotBetween compilation.
     *
     * @return void
     */
    public function test_where_not_between_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereNotBetween('id', [1, 100]);
        $sql = $builder->toSql();

        $this->assertStringContainsString('"id" NOT BETWEEN ? AND ?', $sql);
    }

    /**
     * Test whereBetweenColumns compilation.
     *
     * @return void
     */
    public function test_where_between_columns_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereBetweenColumns('amount', ['min_amount', 'max_amount']);
        $sql = $builder->toSql();

        $this->assertStringContainsString('"amount" BETWEEN "min_amount" AND "max_amount"', $sql);
    }

    /**
     * Test whereNotBetweenColumns compilation.
     *
     * @return void
     */
    public function test_where_not_between_columns_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereNotBetweenColumns('amount', ['min_amount', 'max_amount']);
        $sql = $builder->toSql();

        $this->assertStringContainsString('"amount" NOT BETWEEN "min_amount" AND "max_amount"', $sql);
    }

    /**
     * Test whereNull compilation.
     *
     * @return void
     */
    public function test_where_null_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereNull('deleted_at');
        $sql = $builder->toSql();

        $this->assertStringContainsString('"deleted_at" IS NULL', $sql);
    }

    /**
     * Test whereNotNull compilation.
     *
     * @return void
     */
    public function test_where_not_null_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereNotNull('deleted_at');
        $sql = $builder->toSql();

        $this->assertStringContainsString('"deleted_at" IS NOT NULL', $sql);
    }

    /**
     * Test whereLike compilation.
     *
     * @return void
     */
    public function test_where_like_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereLike('name', '%john%');
        $sql = $builder->toSql();

        $this->assertStringContainsString('"name" like ?', $sql);
        $this->assertContains('%john%', $builder->getBindings());
    }

    /**
     * Test whereILike compilation.
     *
     * @return void
     */
    public function test_where_ilike_compilation(): void
    {
        $builder = $this->builder->from('test_table')->whereILike('name', '%john%');
        $sql = $builder->toSql();

        $this->assertStringContainsString('"name" ilike ?', $sql);
    }

    /**
     * Test complex query with multiple where types.
     *
     * @return void
     */
    public function test_complex_query_compilation(): void
    {
        $builder = $this->builder
            ->from('test_table')
            ->whereDate('created_at', '>=', '2025-09-01')
            ->whereBetween('id', [1, 1000])
            ->whereNotNull('name')
            ->whereYear('updated_at', 2025)
            ->limit(100);

        $sql = $builder->toSql();

        $this->assertStringContainsString('toDate("created_at") >= ?', $sql);
        $this->assertStringContainsString('"id" BETWEEN ? AND ?', $sql);
        $this->assertStringContainsString('"name" IS NOT NULL', $sql);
        $this->assertStringContainsString('toYear("updated_at") = ?', $sql);
        $this->assertStringContainsString('LIMIT 100', $sql);
    }

    /**
     * Test database prefixing in table wrapping.
     *
     * @return void
     */
    public function test_table_wrapping_with_database_prefix(): void
    {
        $builder = $this->builder->from('test_table')->whereDate('created_at', '2025-09-07');
        $sql = $builder->toSql();

        // The table should be wrapped with database prefix
        $this->assertStringContainsString('FROM "test_db"."test_table"', $sql);
    }

    /**
     * Test OR conditions with date functions.
     *
     * @return void
     */
    public function test_or_conditions_with_date_functions(): void
    {
        $builder = $this->builder
            ->from('test_table')
            ->whereDate('created_at', '2025-09-07')
            ->orWhereMonth('created_at', 8);

        $sql = $builder->toSql();

        $this->assertStringContainsString('toDate("created_at") = ?', $sql);
        $this->assertStringContainsString('or toMonth("created_at") = ?', $sql);
    }

    /**
     * Test multiple between conditions.
     *
     * @return void
     */
    public function test_multiple_between_conditions(): void
    {
        $builder = $this->builder
            ->from('test_table')
            ->whereBetween('price', [100, 500])
            ->whereBetween('quantity', [1, 10]);

        $sql = $builder->toSql();

        $this->assertStringContainsString('"price" BETWEEN ? AND ?', $sql);
        $this->assertStringContainsString('and "quantity" BETWEEN ? AND ?', $sql);
    }

    /**
     * Test date range query (real-world example).
     *
     * @return void
     */
    public function test_date_range_query(): void
    {
        $builder = $this->builder
            ->from('analytics_events')
            ->whereDate('created_at', '>=', '2025-09-01')
            ->whereDate('created_at', '<=', '2025-09-07')
            ->whereBetween('user_id', [1, 10000])
            ->whereNotNull('event_data')
            ->limit(1000);

        $sql = $builder->toSql();

        $this->assertStringContainsString('toDate("created_at") >= ?', $sql);
        $this->assertStringContainsString('and toDate("created_at") <= ?', $sql);
        $this->assertStringContainsString('and "user_id" BETWEEN ? AND ?', $sql);
        $this->assertStringContainsString('and "event_data" IS NOT NULL', $sql);
        $this->assertStringContainsString('LIMIT 1000', $sql);

        // Verify bindings
        $bindings = $builder->getBindings();
        $this->assertContains('2025-09-01', $bindings);
        $this->assertContains('2025-09-07', $bindings);
        $this->assertEquals([1, 10000], $bindings['where'][2]);
    }
}