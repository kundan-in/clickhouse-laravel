<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use InvalidArgumentException;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Database\ClickHouseQueryBuilder;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use Mockery;

/**
 * ClickHouse Query Builder Methods Test
 *
 * Tests the Laravel query builder methods like whereDate, whereBetween, etc.
 */
class ClickHouseQueryBuilderMethodsTest extends TestCase
{
    protected ClickHouseConnection $connection;
    protected ClickHouseQueryBuilder $builder;

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
    }

    /**
     * Test whereDate method.
     *
     * @return void
     */
    public function test_where_date(): void
    {
        $builder = $this->builder->from('test_table')->whereDate('created_at', '2025-09-07');

        $this->assertContains([
            'type' => 'Date',
            'column' => 'created_at',
            'operator' => '=',
            'value' => '2025-09-07',
            'boolean' => 'and',
        ], $builder->wheres);

        $this->assertContains('2025-09-07', $builder->getBindings());
    }

    /**
     * Test whereDate method with operator.
     *
     * @return void
     */
    public function test_where_date_with_operator(): void
    {
        $builder = $this->builder->from('test_table')->whereDate('created_at', '<=', '2025-09-07');

        $this->assertContains([
            'type' => 'Date',
            'column' => 'created_at',
            'operator' => '<=',
            'value' => '2025-09-07',
            'boolean' => 'and',
        ], $builder->wheres);

        $this->assertContains('2025-09-07', $builder->getBindings());
    }

    /**
     * Test whereTime method.
     *
     * @return void
     */
    public function test_where_time(): void
    {
        $builder = $this->builder->from('test_table')->whereTime('created_at', '14:30:00');

        $this->assertContains([
            'type' => 'Time',
            'column' => 'created_at',
            'operator' => '=',
            'value' => '14:30:00',
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereDay method.
     *
     * @return void
     */
    public function test_where_day(): void
    {
        $builder = $this->builder->from('test_table')->whereDay('created_at', 7);

        $this->assertContains([
            'type' => 'Day',
            'column' => 'created_at',
            'operator' => '=',
            'value' => 7,
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereMonth method.
     *
     * @return void
     */
    public function test_where_month(): void
    {
        $builder = $this->builder->from('test_table')->whereMonth('created_at', 9);

        $this->assertContains([
            'type' => 'Month',
            'column' => 'created_at',
            'operator' => '=',
            'value' => 9,
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereYear method.
     *
     * @return void
     */
    public function test_where_year(): void
    {
        $builder = $this->builder->from('test_table')->whereYear('created_at', 2025);

        $this->assertContains([
            'type' => 'Year',
            'column' => 'created_at',
            'operator' => '=',
            'value' => 2025,
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereBetween method.
     *
     * @return void
     */
    public function test_where_between(): void
    {
        $builder = $this->builder->from('test_table')->whereBetween('created_at', ['2025-09-01', '2025-09-07']);

        $this->assertContains([
            'type' => 'Between',
            'column' => 'created_at',
            'values' => ['2025-09-01', '2025-09-07'],
            'boolean' => 'and',
        ], $builder->wheres);

        // Check that both values are present in the bindings somewhere
        $allBindings = collect($builder->getBindings())->flatten()->all();
        $this->assertContains('2025-09-01', $allBindings);
        $this->assertContains('2025-09-07', $allBindings);
    }

    /**
     * Test whereNotBetween method.
     *
     * @return void
     */
    public function test_where_not_between(): void
    {
        $builder = $this->builder->from('test_table')->whereNotBetween('created_at', ['2025-09-01', '2025-09-07']);

        $this->assertContains([
            'type' => 'NotBetween',
            'column' => 'created_at',
            'values' => ['2025-09-01', '2025-09-07'],
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereBetween with invalid values throws exception.
     *
     * @return void
     */
    public function test_where_between_invalid_values(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('whereBetween expects exactly 2 values.');

        $this->builder->from('test_table')->whereBetween('created_at', ['2025-09-01']);
    }

    /**
     * Test whereBetweenColumns method.
     *
     * @return void
     */
    public function test_where_between_columns(): void
    {
        $builder = $this->builder->from('test_table')->whereBetweenColumns('amount', ['min_amount', 'max_amount']);

        $this->assertContains([
            'type' => 'BetweenColumns',
            'column' => 'amount',
            'values' => ['min_amount', 'max_amount'],
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereNotBetweenColumns method.
     *
     * @return void
     */
    public function test_where_not_between_columns(): void
    {
        $builder = $this->builder->from('test_table')->whereNotBetweenColumns('amount', ['min_amount', 'max_amount']);

        $this->assertContains([
            'type' => 'NotBetweenColumns',
            'column' => 'amount',
            'values' => ['min_amount', 'max_amount'],
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereNull method.
     *
     * @return void
     */
    public function test_where_null(): void
    {
        $builder = $this->builder->from('test_table')->whereNull('deleted_at');

        $this->assertContains([
            'type' => 'Null',
            'column' => 'deleted_at',
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereNull method with array of columns.
     *
     * @return void
     */
    public function test_where_null_array(): void
    {
        $builder = $this->builder->from('test_table')->whereNull(['deleted_at', 'archived_at']);

        $this->assertContains([
            'type' => 'Null',
            'column' => 'deleted_at',
            'boolean' => 'and',
        ], $builder->wheres);

        $this->assertContains([
            'type' => 'Null',
            'column' => 'archived_at',
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereNotNull method.
     *
     * @return void
     */
    public function test_where_not_null(): void
    {
        $builder = $this->builder->from('test_table')->whereNotNull('deleted_at');

        $this->assertContains([
            'type' => 'NotNull',
            'column' => 'deleted_at',
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereLike method.
     *
     * @return void
     */
    public function test_where_like(): void
    {
        $builder = $this->builder->from('test_table')->whereLike('name', '%john%');

        $this->assertContains([
            'type' => 'Basic',
            'column' => 'name',
            'operator' => 'like',
            'value' => '%john%',
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereNotLike method.
     *
     * @return void
     */
    public function test_where_not_like(): void
    {
        $builder = $this->builder->from('test_table')->whereNotLike('name', '%john%');

        $this->assertContains([
            'type' => 'Basic',
            'column' => 'name',
            'operator' => 'not like',
            'value' => '%john%',
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereILike method.
     *
     * @return void
     */
    public function test_where_ilike(): void
    {
        $builder = $this->builder->from('test_table')->whereILike('name', '%john%');

        $this->assertContains([
            'type' => 'Basic',
            'column' => 'name',
            'operator' => 'ilike',
            'value' => '%john%',
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test whereNotILike method.
     *
     * @return void
     */
    public function test_where_not_ilike(): void
    {
        $builder = $this->builder->from('test_table')->whereNotILike('name', '%john%');

        $this->assertContains([
            'type' => 'Basic',
            'column' => 'name',
            'operator' => 'not ilike',
            'value' => '%john%',
            'boolean' => 'and',
        ], $builder->wheres);
    }

    /**
     * Test chaining multiple where methods.
     *
     * @return void
     */
    public function test_chaining_where_methods(): void
    {
        $builder = $this->builder
            ->from('test_table')
            ->whereDate('created_at', '<=', '2025-09-07')
            ->whereBetween('id', [1, 100])
            ->whereNotNull('name')
            ->limit(100);

        $this->assertCount(3, $builder->wheres);
        $this->assertEquals(100, $builder->limit);
        
        // Verify all where clauses are present
        $this->assertTrue(collect($builder->wheres)->contains('type', 'Date'));
        $this->assertTrue(collect($builder->wheres)->contains('type', 'Between'));
        $this->assertTrue(collect($builder->wheres)->contains('type', 'NotNull'));
    }

    /**
     * Test toArray method works with query builder.
     *
     * @return void
     */
    public function test_to_array_compatibility(): void
    {
        // This tests that the query builder can be used with ->toArray()
        // which would typically be called on the result collection
        $builder = $this->builder
            ->from('test_table')
            ->whereDate('created_at', '<=', '2025-09-07')
            ->limit(100);

        // Verify the SQL is generated correctly
        $sql = $builder->toSql();
        
        $this->assertStringContainsString('toDate("created_at") <= ?', $sql);
        $this->assertStringContainsString('LIMIT 100', $sql);
    }

    /**
     * Test prepareValueAndOperator method.
     *
     * @return void
     */
    public function test_prepare_value_and_operator(): void
    {
        $reflection = new \ReflectionClass($this->builder);
        $method = $reflection->getMethod('prepareValueAndOperator');
        $method->setAccessible(true);

        // Test with default operator (when useDefault is true, returns [operator, '='])
        [$value, $operator] = $method->invoke($this->builder, null, '2025-09-07', true);
        $this->assertEquals('2025-09-07', $value);
        $this->assertEquals('=', $operator);

        // Test with explicit operator
        [$value, $operator] = $method->invoke($this->builder, '2025-09-07', '<=', false);
        $this->assertEquals('2025-09-07', $value);
        $this->assertEquals('<=', $operator);
    }

    /**
     * Clean up after each test.
     *
     * @return void
     */
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
}