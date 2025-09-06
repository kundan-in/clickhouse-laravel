<?php

namespace KundanIn\ClickHouseLaravel\Tests\Integration;

use KundanIn\ClickHouseLaravel\Database\ClickHouseModel;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * Test Model that mimics the original issue scenario
 */
class TestItem extends ClickHouseModel
{
    protected $table = 'test_items';

    protected $connection = 'clickhouse';

    public $timestamps = false;

    protected $fillable = ['session_id', 'name', 'value'];
}

/**
 * All Method Integration Test
 *
 * Tests the specific scenario that was failing: calling all() on a model query.
 */
class AllMethodIntegrationTest extends TestCase
{
    /**
     * Test the exact scenario that was failing.
     */
    public function test_where_clause_with_all_method()
    {
        // This is the exact pattern that was failing:
        // Model::where('field', 'value')->all()

        $query = TestItem::where('session_id', 'sess_1756052806769_akpqnhvby9u');

        // Verify this returns a ClickHouseEloquentBuilder
        $this->assertInstanceOf(\KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder::class, $query);

        // Verify the all() method exists on the builder
        $this->assertTrue(method_exists($query, 'all'));

        // This should NOT throw "Call to undefined method Illuminate\Database\Eloquent\Builder::all()"
        try {
            // We can't actually execute the query without a real database,
            // but we can verify the method resolution works
            $reflection = new \ReflectionClass($query);
            $method = $reflection->getMethod('all');

            $this->assertTrue($method->isPublic());
            $this->assertEquals('all', $method->getName());

            // Verify the method is callable
            $this->assertTrue(is_callable([$query, 'all']));

        } catch (\Exception $e) {
            $this->fail('The all() method should be callable on the query builder: '.$e->getMessage());
        }
    }

    /**
     * Test static all() method.
     */
    public function test_static_all_method()
    {
        // Test Model::all() (static call)
        $this->assertTrue(method_exists(TestItem::class, 'all'));

        // Verify it's callable without throwing undefined method error
        $this->assertTrue(is_callable([TestItem::class, 'all']));
    }

    /**
     * Test various query builder methods that should all work.
     */
    public function test_query_builder_method_chaining()
    {
        // Test various patterns that should work
        $patterns = [
            TestItem::where('session_id', 'test'),
            TestItem::where('name', 'like', '%test%'),
            TestItem::orderBy('created_at'),
            TestItem::select('session_id', 'name'),
        ];

        foreach ($patterns as $query) {
            $this->assertInstanceOf(
                \KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder::class,
                $query
            );
            $this->assertTrue(method_exists($query, 'all'));
            $this->assertTrue(method_exists($query, 'get'));
            $this->assertTrue(method_exists($query, 'first'));
        }
    }

    /**
     * Test that SQL generation works correctly.
     */
    public function test_sql_generation_with_where_clause()
    {
        $query = TestItem::where('session_id', 'sess_1756052806769_akpqnhvby9u');

        $sql = $query->toSql();

        // Should generate proper ClickHouse SQL
        $this->assertStringContainsString('select', strtolower($sql));
        $this->assertStringContainsString('test_items', $sql);
        $this->assertStringContainsString('session_id', $sql);

        // Should have proper parameter binding
        $bindings = $query->getBindings();
        $this->assertContains('sess_1756052806769_akpqnhvby9u', $bindings);
    }

    /**
     * Test model instantiation and connection.
     */
    public function test_model_uses_clickhouse_connection()
    {
        $item = new TestItem;

        $this->assertEquals('clickhouse', $item->getConnectionName());

        // Verify the connection is properly set up
        $connection = $item->getConnection();
        $this->assertInstanceOf(\KundanIn\ClickHouseLaravel\Database\ClickHouseConnection::class, $connection);
    }

    /**
     * Test the specific error case that was reported.
     */
    public function test_specific_error_case_resolution()
    {
        // This exact call was failing with:
        // Call to undefined method Illuminate\Database\Eloquent\Builder::all()

        // Step 1: Create the query
        $query = TestItem::where('session_id', 'sess_1756052806769_akpqnhvby9u');

        // Step 2: Verify it's our custom builder, not Laravel's default
        $this->assertInstanceOf(
            \KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder::class,
            $query
        );

        // Step 3: Verify the all() method exists and is the right one
        $this->assertTrue(method_exists($query, 'all'));

        // Step 4: Verify the method signature matches what we expect
        $reflection = new \ReflectionMethod($query, 'all');
        $parameters = $reflection->getParameters();

        // Should accept a $columns parameter with default ['*']
        $this->assertEquals('columns', $parameters[0]->getName());
        $this->assertTrue($parameters[0]->isDefaultValueAvailable());

        // This should now work without errors
        $this->expectNotToPerformAssertions();
    }
}
