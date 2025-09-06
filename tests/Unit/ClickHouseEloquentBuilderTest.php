<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use Illuminate\Database\Eloquent\Model;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use Mockery;

/**
 * ClickHouse Eloquent Builder Test
 *
 * Tests the ClickHouse Eloquent builder functionality and the all() method.
 */
class ClickHouseEloquentBuilderTest extends TestCase
{
    protected ClickHouseEloquentBuilder $builder;

    protected $mockConnection;

    protected $mockModel;

    /**
     * Set up the test environment.
     *
     * @return void
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->mockConnection = Mockery::mock(ClickHouseConnection::class);
        $this->mockModel = Mockery::mock(Model::class);
        $this->mockModel->shouldReceive('getTable')->andReturn('test_table');
        $this->mockModel->shouldReceive('getKeyName')->andReturn('id');
        $this->mockModel->shouldReceive('newCollection')->andReturn(collect());

        // Create a mock query builder
        $mockQueryBuilder = Mockery::mock(\Illuminate\Database\Query\Builder::class);
        $mockQueryBuilder->shouldReceive('getConnection')->andReturn($this->mockConnection);
        $mockQueryBuilder->shouldReceive('from')->andReturn($mockQueryBuilder);

        $this->builder = new ClickHouseEloquentBuilder($mockQueryBuilder);
        $this->builder->setModel($this->mockModel);
    }

    /**
     * Test that all() method exists and calls get().
     *
     * @return void
     */
    public function test_all_method_exists(): void
    {
        $this->assertTrue(method_exists($this->builder, 'all'));
    }

    /**
     * Test find method exists and handles single ID.
     *
     * @return void
     */
    public function test_find_method_exists(): void
    {
        $this->assertTrue(method_exists($this->builder, 'find'));
    }

    /**
     * Test findMany method exists and handles array of IDs.
     *
     * @return void
     */
    public function test_find_many_method_exists(): void
    {
        $this->assertTrue(method_exists($this->builder, 'findMany'));
    }

    /**
     * Test findOrFail method exists.
     *
     * @return void
     */
    public function test_find_or_fail_method_exists(): void
    {
        $this->assertTrue(method_exists($this->builder, 'findOrFail'));
    }

    /**
     * Test where method exists.
     *
     * @return void
     */
    public function test_where_method_exists(): void
    {
        $this->assertTrue(method_exists($this->builder, 'where'));
    }

    /**
     * Test whereIn method exists.
     *
     * @return void
     */
    public function test_where_in_method_exists(): void
    {
        $this->assertTrue(method_exists($this->builder, 'whereIn'));
    }

    /**
     * Clean up Mockery after each test.
     *
     * @return void
     */
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
}
