<?php

namespace KundanIn\ClickHouseLaravel\Tests\Integration;

use KundanIn\ClickHouseLaravel\Database\ClickHouseModel;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * Test Model for ClickHouse integration tests
 */
class TestWebVisitorEvent extends ClickHouseModel
{
    protected $table = 'web_visitor_events';

    public $timestamps = false;

    protected $fillable = ['session_id', 'event_type', 'url'];
}

/**
 * Eloquent All Method Integration Test
 *
 * Tests the integration between ClickHouse and Eloquent's all() method.
 */
class EloquentAllMethodTest extends TestCase
{
    /**
     * Test that all() method works with ClickHouse Eloquent models.
     *
     * @return void
     */
    public function test_eloquent_all_method_integration(): void
    {
        // This test mainly verifies that the method exists and doesn't throw an error
        // We don't need actual data since we're testing the method existence

        $model = new TestWebVisitorEvent;
        $query = $model->where('session_id', 'sess_test_123');

        // The key test: verify all() method exists and can be called
        $this->assertTrue(method_exists($query, 'all'));

        // We can't easily test the actual query execution without a real ClickHouse connection,
        // but we can test that the method chain works without throwing exceptions
        try {
            // This should not throw a "Call to undefined method" exception
            $queryString = $query->toSql();
            $this->assertStringContainsString('session_id', $queryString);
        } catch (\Exception $e) {
            $this->fail('Query building failed: '.$e->getMessage());
        }
    }

    /**
     * Test that where() chaining works before calling all().
     *
     * @return void
     */
    public function test_where_chaining_before_all(): void
    {
        $model = new TestWebVisitorEvent;

        // Test method chaining that should work
        $query = $model->where('session_id', 'sess_1756052806769_akpqnhvby9u')
            ->where('event_type', 'page_view');

        $this->assertTrue(method_exists($query, 'all'));
        $this->assertTrue(method_exists($query, 'get'));
        $this->assertTrue(method_exists($query, 'first'));

        // Test SQL generation
        $sql = $query->toSql();
        $this->assertStringContainsString('session_id', $sql);
        $this->assertStringContainsString('event_type', $sql);
    }
}
