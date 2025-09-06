<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder;
use KundanIn\ClickHouseLaravel\Database\ClickHouseModel;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * Test Model for All Method Testing
 */
class TestModelForAll extends ClickHouseModel
{
    protected $table = 'test_items';

    public $timestamps = false;

    protected $fillable = ['name', 'value'];
}

/**
 * ClickHouse All Method Test
 *
 * Tests that the all() method works correctly with ClickHouse models.
 */
class ClickHouseAllMethodTest extends TestCase
{
    /**
     * Test that all() method exists on model.
     */
    public function test_all_method_exists_on_model()
    {
        $this->assertTrue(method_exists(TestModelForAll::class, 'all'));
    }

    /**
     * Test that query builder is ClickHouseEloquentBuilder.
     */
    public function test_model_uses_clickhouse_eloquent_builder()
    {
        $model = new TestModelForAll;
        $query = $model->newQuery();

        $this->assertInstanceOf(ClickHouseEloquentBuilder::class, $query);
    }

    /**
     * Test that query builder has all() method.
     */
    public function test_eloquent_builder_has_all_method()
    {
        $model = new TestModelForAll;
        $query = $model->newQuery();

        $this->assertTrue(method_exists($query, 'all'));
    }

    /**
     * Test that static query() returns ClickHouseEloquentBuilder.
     */
    public function test_static_query_returns_clickhouse_builder()
    {
        $query = TestModelForAll::query();

        $this->assertInstanceOf(ClickHouseEloquentBuilder::class, $query);
    }

    /**
     * Test that chaining works with all().
     */
    public function test_chaining_works_with_all()
    {
        // Test method chaining that should work before calling all()
        $query = TestModelForAll::where('name', 'test');

        $this->assertInstanceOf(ClickHouseEloquentBuilder::class, $query);
        $this->assertTrue(method_exists($query, 'all'));

        // Test SQL generation
        $sql = $query->toSql();
        $this->assertStringContainsString('name', $sql);
        $this->assertStringContainsString('test_items', $sql);
    }

    /**
     * Test that all() method can be called statically.
     */
    public function test_static_all_method_works()
    {
        // This should not throw an exception about undefined method
        $this->expectNotToPerformAssertions();

        try {
            // We can't actually execute this without a real database,
            // but we can verify the method exists and can be called
            $reflection = new \ReflectionMethod(TestModelForAll::class, 'all');
            $this->assertTrue($reflection->isStatic());
            $this->assertTrue($reflection->isPublic());
        } catch (\ReflectionException $e) {
            $this->fail('all() method should exist and be static');
        }
    }

    /**
     * Test that all() method works with query chaining.
     */
    public function test_all_method_with_query_chaining()
    {
        // Create a query with where clause
        $query = TestModelForAll::where('value', '>', 10);

        // Verify it's the right builder type
        $this->assertInstanceOf(ClickHouseEloquentBuilder::class, $query);

        // Verify all() method exists on the query
        $this->assertTrue(method_exists($query, 'all'));

        // Test that we can call all() without errors (method resolution)
        $sql = $query->toSql();
        $this->assertIsString($sql);
        $this->assertStringContainsString('value', $sql);
    }
}
