<?php

namespace KundanIn\ClickHouseLaravel\Tests\Feature;

use KundanIn\ClickHouseLaravel\Casts\ClickHouseArray;
use KundanIn\ClickHouseLaravel\Casts\ClickHouseJson;
use KundanIn\ClickHouseLaravel\Database\ClickHouseBlueprint;
use KundanIn\ClickHouseLaravel\Database\ClickHouseModel;
use KundanIn\ClickHouseLaravel\Database\ClickHouseQueryBuilder;
use KundanIn\ClickHouseLaravel\Database\ClickHouseSchemaBuilder;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * Test Models for Feature Testing - Generic Test Database Structure
 */
class TestAnalyticsEvent extends ClickHouseModel
{
    protected $table = 'test_analytics_events';

    public $timestamps = true;

    protected $fillable = [
        'identifier', 'category_id', 'action_type', 'location', 'extra_data', 'labels', 'attributes',
    ];

    protected function casts(): array
    {
        return [
            'extra_data' => ClickHouseJson::class,
            'labels' => ClickHouseArray::class.':String',
            'attributes' => ClickHouseArray::class.':String',
            'created_at' => 'datetime',
            'updated_at' => 'datetime',
        ];
    }

    public function category()
    {
        return $this->belongsTo(TestCategory::class, 'category_id');
    }
}

class TestCategory extends ClickHouseModel
{
    protected $table = 'test_categories';

    public $timestamps = true;

    protected $fillable = ['title', 'description', 'configuration'];

    protected function casts(): array
    {
        return [
            'configuration' => ClickHouseJson::class,
        ];
    }

    public function events()
    {
        return $this->hasMany(TestAnalyticsEvent::class, 'category_id');
    }
}

/**
 * ClickHouse Feature Test
 *
 * This test demonstrates the comprehensive functionality of the ClickHouse Laravel package.
 */
class ClickHouseFeatureTest extends TestCase
{
    /**
     * Test schema builder with ClickHouse engines.
     */
    public function test_schema_builder_creates_clickhouse_tables()
    {
        // Test that ClickHouseSchemaBuilder exists and can be instantiated
        $schemaBuilder = $this->app['db']->connection('clickhouse')->getSchemaBuilder();

        $this->assertInstanceOf(ClickHouseSchemaBuilder::class, $schemaBuilder);

        // Test blueprint creation
        $blueprint = new ClickHouseBlueprint('test_table');
        $this->assertInstanceOf(ClickHouseBlueprint::class, $blueprint);
    }

    /**
     * Test query builder with ClickHouse-specific features.
     */
    public function test_query_builder_supports_clickhouse_features()
    {
        $connection = $this->app['db']->connection('clickhouse');
        $queryBuilder = $connection->query();

        $this->assertInstanceOf(ClickHouseQueryBuilder::class, $queryBuilder);

        // Test ClickHouse-specific methods exist
        $this->assertTrue(method_exists($queryBuilder, 'whereArrayHas'));
        $this->assertTrue(method_exists($queryBuilder, 'sample'));
        $this->assertTrue(method_exists($queryBuilder, 'final'));
        $this->assertTrue(method_exists($queryBuilder, 'prewhere'));
        $this->assertTrue(method_exists($queryBuilder, 'uniq'));
        $this->assertTrue(method_exists($queryBuilder, 'uniqExact'));
    }

    /**
     * Test ClickHouse model functionality.
     */
    public function test_clickhouse_model_functionality()
    {
        $event = new TestAnalyticsEvent;

        // Test that it uses ClickHouse connection
        $this->assertEquals('clickhouse', $event->getConnectionName());

        // Test that it has the right casts
        $this->assertArrayHasKey('extra_data', $event->getCasts());
        $this->assertArrayHasKey('labels', $event->getCasts());

        // Test cast configuration
        $this->assertStringContainsString(ClickHouseJson::class, $event->getCasts()['extra_data']);
        $this->assertStringContainsString(ClickHouseArray::class, $event->getCasts()['labels']);

        // Test relationship methods exist
        $this->assertTrue(method_exists($event, 'category'));

        // Test soft delete methods exist
        $this->assertTrue(method_exists($event, 'trashed'));
        $this->assertTrue(method_exists($event, 'restore'));

        // Test helper casting methods
        $this->assertTrue(method_exists($event, 'castAsArray'));
        $this->assertTrue(method_exists($event, 'castAsJson'));
    }

    /**
     * Test ClickHouse array cast functionality.
     */
    public function test_clickhouse_array_cast()
    {
        $cast = new ClickHouseArray('String');

        // Test array formatting for ClickHouse
        $testArray = ['label1', 'label2', 'label3'];
        $formatted = $cast->set(new TestAnalyticsEvent, 'labels', $testArray, []);

        $this->assertEquals("['label1','label2','label3']", $formatted);

        // Test array parsing from ClickHouse
        $parsed = $cast->get(new TestAnalyticsEvent, 'labels', "['label1','label2','label3']", []);

        $this->assertEquals($testArray, $parsed);
    }

    /**
     * Test ClickHouse JSON cast functionality.
     */
    public function test_clickhouse_json_cast()
    {
        $cast = new ClickHouseJson;

        // Test JSON formatting for ClickHouse
        $testData = ['setting1' => 'option1', 'setting2' => 'option2'];
        $formatted = $cast->set(new TestAnalyticsEvent, 'extra_data', $testData, []);

        $this->assertEquals('{"setting1":"option1","setting2":"option2"}', $formatted);

        // Test JSON parsing from ClickHouse
        $parsed = $cast->get(new TestAnalyticsEvent, 'extra_data', '{"setting1":"option1","setting2":"option2"}', []);

        $this->assertEquals($testData, $parsed);
    }

    /**
     * Test query building functionality.
     */
    public function test_query_building_functionality()
    {
        $event = new TestAnalyticsEvent;

        // Test basic query building
        $query = $event->where('identifier', 'test_123')
            ->where('action_type', 'view');

        $sql = $query->toSql();

        $this->assertStringContainsString('identifier', $sql);
        $this->assertStringContainsString('action_type', $sql);
        $this->assertStringContainsString('test_analytics_events', $sql);
    }

    /**
     * Test relationship functionality.
     */
    public function test_relationship_functionality()
    {
        $category = new TestCategory;
        $event = new TestAnalyticsEvent;

        // Test hasMany relationship
        $eventsRelation = $category->events();
        $this->assertInstanceOf(\Illuminate\Database\Eloquent\Relations\HasMany::class, $eventsRelation);

        // Test belongsTo relationship
        $categoryRelation = $event->category();
        $this->assertInstanceOf(\Illuminate\Database\Eloquent\Relations\BelongsTo::class, $categoryRelation);
    }

    /**
     * Test exception handling.
     */
    public function test_exception_handling()
    {
        $queryBuilder = new ClickHouseQueryBuilder(
            $this->app['db']->connection('clickhouse'),
            $this->app['db']->connection('clickhouse')->getQueryGrammar(),
            $this->app['db']->connection('clickhouse')->getPostProcessor()
        );

        // Test that unsupported joins throw exceptions
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('RIGHT JOIN is not supported in ClickHouse');

        $queryBuilder->rightJoin('other_table', 'id', '=', 'other_id');
    }

    /**
     * Test ClickHouse-specific query methods.
     */
    public function test_clickhouse_specific_query_methods()
    {
        $queryBuilder = new ClickHouseQueryBuilder(
            $this->app['db']->connection('clickhouse'),
            $this->app['db']->connection('clickhouse')->getQueryGrammar(),
            $this->app['db']->connection('clickhouse')->getPostProcessor()
        );

        // Test sample method
        $result = $queryBuilder->from('test_table')->sample(0.1);
        $this->assertInstanceOf(ClickHouseQueryBuilder::class, $result);

        // Test final method
        $result = $queryBuilder->from('test_table')->final();
        $this->assertInstanceOf(ClickHouseQueryBuilder::class, $result);

        // Test array methods
        $result = $queryBuilder->whereArrayHas('tags', 'test_tag');
        $this->assertInstanceOf(ClickHouseQueryBuilder::class, $result);
    }

    /**
     * Test complete feature integration.
     */
    public function test_complete_feature_integration()
    {
        // Test that we can create a model with all features
        $event = new TestAnalyticsEvent([
            'identifier' => 'test_item_123',
            'category_id' => 1,
            'action_type' => 'click',
            'location' => 'https://testsite.com',
            'extra_data' => ['device' => 'mobile', 'platform' => 'android'],
            'labels' => ['feature', 'experiment', 'test'],
            'attributes' => ['param1', 'param2', 'param3'],
        ]);

        // Test that attributes are properly cast
        $this->assertEquals('test_item_123', $event->identifier);
        $this->assertEquals(['device' => 'mobile', 'platform' => 'android'], $event->extra_data);
        $this->assertEquals(['feature', 'experiment', 'test'], $event->labels);

        // Test that we can build complex queries
        $query = TestAnalyticsEvent::where('identifier', 'test_item_123')
            ->where('action_type', 'click')
            ->with('category');

        $sql = $query->toSql();

        // Should contain ClickHouse-compatible SQL
        $this->assertStringContainsString('select', strtolower($sql));
        $this->assertStringContainsString('test_analytics_events', $sql);
        $this->assertStringContainsString('identifier', $sql);
    }
}
