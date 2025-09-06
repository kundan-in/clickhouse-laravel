<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder;
use KundanIn\ClickHouseLaravel\Database\ClickHouseModel;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * Simplest possible test model
 */
class SimpleTestModel extends ClickHouseModel
{
    protected $table = 'simple_test';

    public $timestamps = false;

    // Override to force our builder creation
    public function newQuery()
    {
        $builder = $this->registerGlobalScopes($this->newQueryWithoutScopes());

        // Log what we're returning for debugging
        if (app()->environment('testing')) {
            logger('ClickHouse newQuery returning: '.get_class($builder));
        }

        return $builder;
    }
}

/**
 * Simple test to isolate the all() method issue
 */
class ClickHouseAllMethodSimpleTest extends TestCase
{
    /**
     * Test the exact builder chain that's failing.
     */
    public function test_builder_chain_returns_correct_type()
    {
        // Step 1: Verify newQuery returns our builder
        $model = new SimpleTestModel;
        $query1 = $model->newQuery();

        $this->assertInstanceOf(ClickHouseEloquentBuilder::class, $query1);
        $this->assertTrue(method_exists($query1, 'all'));

        // Step 2: Verify static query() returns our builder
        $query2 = SimpleTestModel::query();

        $this->assertInstanceOf(ClickHouseEloquentBuilder::class, $query2);
        $this->assertTrue(method_exists($query2, 'all'));

        // Step 3: Test where() chain - this is where it might break
        try {
            $query3 = SimpleTestModel::where('field', 'value');

            $this->assertInstanceOf(
                ClickHouseEloquentBuilder::class,
                $query3,
                'Expected ClickHouseEloquentBuilder, got: '.get_class($query3)
            );

            $this->assertTrue(
                method_exists($query3, 'all'),
                'all() method should exist on: '.get_class($query3)
            );

        } catch (\Exception $e) {
            $this->fail('where() method failed: '.$e->getMessage());
        }
    }

    /**
     * Test all method exists and is callable on chained query
     */
    public function test_chained_query_all_method()
    {
        $query = SimpleTestModel::where('test_field', 'test_value');

        // Debug: what class is this?
        $actualClass = get_class($query);

        $this->assertInstanceOf(
            ClickHouseEloquentBuilder::class,
            $query,
            "Expected ClickHouseEloquentBuilder but got {$actualClass}"
        );

        // Check if all() method exists
        $hasAllMethod = method_exists($query, 'all');
        $this->assertTrue($hasAllMethod, "all() method missing on {$actualClass}");

        // Check if it's callable
        $isCallable = is_callable([$query, 'all']);
        $this->assertTrue($isCallable, "all() method not callable on {$actualClass}");
    }

    /**
     * Test builder creation methods step by step
     */
    public function test_builder_creation_step_by_step()
    {
        $model = new SimpleTestModel;

        // Test newBaseQueryBuilder
        $baseBuilder = $model->newQuery()->getQuery();
        $this->assertInstanceOf(\KundanIn\ClickHouseLaravel\Database\ClickHouseQueryBuilder::class, $baseBuilder);

        // Test newEloquentBuilder
        $eloquentBuilder = $model->newEloquentBuilder($baseBuilder);
        $this->assertInstanceOf(ClickHouseEloquentBuilder::class, $eloquentBuilder);

        // Test that it has all() method
        $this->assertTrue(method_exists($eloquentBuilder, 'all'));
    }
}
