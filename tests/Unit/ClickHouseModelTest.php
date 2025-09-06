<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use ClickHouseDB\Client;
use Illuminate\Database\Eloquent\Model;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use Mockery;

/**
 * Test Model for ClickHouse operations
 */
class WebVisitorEvent extends Model
{
    protected $connection = 'clickhouse';

    protected $table = 'web_visitor_events';

    public $timestamps = false;

    protected $fillable = [
        'visitor_id',
        'event_type',
        'url',
        'user_agent',
        'ip_address',
        'session_id',
        'created_at',
    ];
}

/**
 * ClickHouse Model Test
 *
 * Comprehensive tests for Laravel Eloquent model operations with ClickHouse.
 * Tests all major model methods to ensure compatibility.
 */
class ClickHouseModelTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->setupClickHouseConnection();
    }

    /**
     * Test model can be instantiated.
     */
    public function test_model_instantiation(): void
    {
        $event = new WebVisitorEvent;

        $this->assertInstanceOf(WebVisitorEvent::class, $event);
        $this->assertEquals('clickhouse', $event->getConnectionName());
        $this->assertEquals('web_visitor_events', $event->getTable());
    }

    /**
     * Test model all() method.
     */
    public function test_model_all_method(): void
    {
        $mockData = [
            ['id' => 1, 'visitor_id' => 'vis_123', 'event_type' => 'page_view'],
            ['id' => 2, 'visitor_id' => 'vis_124', 'event_type' => 'click'],
        ];

        $this->mockConnectionSelect($mockData);

        $results = WebVisitorEvent::all();

        $this->assertCount(2, $results);
        $this->assertEquals('vis_123', $results->first()->visitor_id);
        $this->assertEquals('click', $results->last()->event_type);
    }

    /**
     * Test model find() method.
     */
    public function test_model_find_method(): void
    {
        $mockData = [['id' => 1, 'visitor_id' => 'vis_123', 'event_type' => 'page_view']];

        $this->mockConnectionSelect($mockData);

        $result = WebVisitorEvent::find(1);

        $this->assertInstanceOf(WebVisitorEvent::class, $result);
        $this->assertEquals(1, $result->id);
        $this->assertEquals('vis_123', $result->visitor_id);
    }

    /**
     * Test model where() method.
     */
    public function test_model_where_method(): void
    {
        $mockData = [['id' => 1, 'visitor_id' => 'vis_123', 'event_type' => 'page_view']];

        $this->mockConnectionSelect($mockData);

        $results = WebVisitorEvent::where('event_type', 'page_view')->get();

        $this->assertCount(1, $results);
        $this->assertEquals('page_view', $results->first()->event_type);
    }

    /**
     * Test model first() method.
     */
    public function test_model_first_method(): void
    {
        $mockData = [['id' => 1, 'visitor_id' => 'vis_123', 'event_type' => 'page_view']];

        $this->mockConnectionSelect($mockData);

        $result = WebVisitorEvent::where('visitor_id', 'vis_123')->first();

        $this->assertInstanceOf(WebVisitorEvent::class, $result);
        $this->assertEquals('vis_123', $result->visitor_id);
    }

    /**
     * Test model count() method.
     */
    public function test_model_count_method(): void
    {
        $mockData = [['aggregate' => 250]];

        $this->mockConnectionSelect($mockData);

        $count = WebVisitorEvent::count();

        $this->assertEquals(250, $count);
    }

    /**
     * Test model limit() method.
     */
    public function test_model_limit_method(): void
    {
        $mockData = [
            ['id' => 1, 'visitor_id' => 'vis_123'],
            ['id' => 2, 'visitor_id' => 'vis_124'],
        ];

        $this->mockConnectionSelect($mockData);

        $results = WebVisitorEvent::limit(2)->get();

        $this->assertCount(2, $results);
    }

    /**
     * Test model orderBy() method.
     */
    public function test_model_order_by_method(): void
    {
        $mockData = [
            ['id' => 2, 'visitor_id' => 'vis_124', 'created_at' => '2023-09-06 10:00:00'],
            ['id' => 1, 'visitor_id' => 'vis_123', 'created_at' => '2023-09-05 10:00:00'],
        ];

        $this->mockConnectionSelect($mockData);

        $results = WebVisitorEvent::orderBy('created_at', 'desc')->get();

        $this->assertEquals(2, $results->first()->id);
        $this->assertEquals(1, $results->last()->id);
    }

    /**
     * Test model pluck() method.
     */
    public function test_model_pluck_method(): void
    {
        $mockData = [
            ['visitor_id' => 'vis_123'],
            ['visitor_id' => 'vis_124'],
        ];

        $this->mockConnectionSelect($mockData);

        $visitorIds = WebVisitorEvent::pluck('visitor_id');

        $this->assertEquals(['vis_123', 'vis_124'], $visitorIds->toArray());
    }

    /**
     * Test model groupBy() method.
     */
    public function test_model_group_by_method(): void
    {
        $mockData = [
            ['event_type' => 'page_view', 'count' => 150],
            ['event_type' => 'click', 'count' => 75],
        ];

        $this->mockConnectionSelect($mockData);

        $results = WebVisitorEvent::selectRaw('event_type, count(*) as count')
            ->groupBy('event_type')
            ->get();

        $this->assertCount(2, $results);
        $this->assertEquals('page_view', $results->first()->event_type);
        $this->assertEquals(150, $results->first()->count);
    }

    /**
     * Test model whereDate() method.
     */
    public function test_model_where_date_method(): void
    {
        $mockData = [['id' => 1, 'visitor_id' => 'vis_123', 'created_at' => '2023-09-05 10:00:00']];

        $this->mockConnectionSelect($mockData);

        $results = WebVisitorEvent::whereDate('created_at', '2023-09-05')->get();

        $this->assertCount(1, $results);
        $this->assertEquals('vis_123', $results->first()->visitor_id);
    }

    /**
     * Test model whereBetween() method.
     */
    public function test_model_where_between_method(): void
    {
        $mockData = [
            ['id' => 1, 'visitor_id' => 'vis_123'],
            ['id' => 2, 'visitor_id' => 'vis_124'],
        ];

        $this->mockConnectionSelect($mockData);

        $results = WebVisitorEvent::whereBetween('created_at', ['2023-09-01', '2023-09-30'])->get();

        $this->assertCount(2, $results);
    }

    /**
     * Test model whereIn() method.
     */
    public function test_model_where_in_method(): void
    {
        $mockData = [
            ['id' => 1, 'event_type' => 'page_view'],
            ['id' => 2, 'event_type' => 'click'],
        ];

        $this->mockConnectionSelect($mockData);

        $results = WebVisitorEvent::whereIn('event_type', ['page_view', 'click'])->get();

        $this->assertCount(2, $results);
    }

    /**
     * Test model distinct() method.
     */
    public function test_model_distinct_method(): void
    {
        $mockData = [
            ['event_type' => 'page_view'],
            ['event_type' => 'click'],
            ['event_type' => 'scroll'],
        ];

        $this->mockConnectionSelect($mockData);

        $results = WebVisitorEvent::distinct()->pluck('event_type');

        $this->assertCount(3, $results);
        $this->assertContains('page_view', $results->toArray());
        $this->assertContains('click', $results->toArray());
        $this->assertContains('scroll', $results->toArray());
    }

    /**
     * Test model exists() method.
     */
    public function test_model_exists_method(): void
    {
        $mockData = [['exists' => 1]];

        $this->mockConnectionSelect($mockData);

        $exists = WebVisitorEvent::where('visitor_id', 'vis_123')->exists();

        $this->assertTrue($exists);
    }

    /**
     * Test model chunk() method.
     */
    public function test_model_chunk_method(): void
    {
        $mockData = [
            ['id' => 1, 'visitor_id' => 'vis_123'],
            ['id' => 2, 'visitor_id' => 'vis_124'],
        ];

        $this->mockConnectionSelect($mockData);

        $processedCount = 0;
        WebVisitorEvent::orderBy('id')->chunk(100, function ($events) use (&$processedCount) {
            $processedCount += count($events);
        });

        $this->assertEquals(2, $processedCount);
    }

    /**
     * Test model max() method.
     */
    public function test_model_max_method(): void
    {
        $mockData = [['aggregate' => 1000]];

        $this->mockConnectionSelect($mockData);

        $maxId = WebVisitorEvent::max('id');

        $this->assertEquals(1000, $maxId);
    }

    /**
     * Test model min() method.
     */
    public function test_model_min_method(): void
    {
        $mockData = [['aggregate' => 1]];

        $this->mockConnectionSelect($mockData);

        $minId = WebVisitorEvent::min('id');

        $this->assertEquals(1, $minId);
    }

    /**
     * Test model avg() method.
     */
    public function test_model_avg_method(): void
    {
        $mockData = [['aggregate' => 42.5]];

        $this->mockConnectionSelect($mockData);

        $avgScore = WebVisitorEvent::avg('score');

        $this->assertEquals(42.5, $avgScore);
    }

    /**
     * Test model sum() method.
     */
    public function test_model_sum_method(): void
    {
        $mockData = [['aggregate' => 12500]];

        $this->mockConnectionSelect($mockData);

        $totalAmount = WebVisitorEvent::sum('amount');

        $this->assertEquals(12500, $totalAmount);
    }

    /**
     * Test model toArray() method.
     */
    public function test_model_to_array_method(): void
    {
        $mockData = [['id' => 1, 'visitor_id' => 'vis_123', 'event_type' => 'page_view']];

        $this->mockConnectionSelect($mockData);

        $results = WebVisitorEvent::limit(1)->get();
        $array = $results->toArray();

        $this->assertIsArray($array);
        $this->assertCount(1, $array);
        $this->assertEquals('vis_123', $array[0]['visitor_id']);
    }

    /**
     * Test model toJson() method.
     */
    public function test_model_to_json_method(): void
    {
        $mockData = [['id' => 1, 'visitor_id' => 'vis_123', 'event_type' => 'page_view']];

        $this->mockConnectionSelect($mockData);

        $results = WebVisitorEvent::limit(1)->get();
        $json = $results->toJson();

        $this->assertJson($json);
        $decoded = json_decode($json, true);
        $this->assertEquals('vis_123', $decoded[0]['visitor_id']);
    }

    /**
     * Setup ClickHouse connection for testing.
     */
    private function setupClickHouseConnection(): void
    {
        config(['database.connections.clickhouse' => [
            'driver' => 'clickhouse',
            'host' => '127.0.0.1',
            'port' => 8123,
            'database' => 'test_database',
            'username' => 'default',
            'password' => '',
        ]]);
    }

    /**
     * Mock the connection select method.
     */
    private function mockConnectionSelect(array $returnData): void
    {
        $mockClient = Mockery::mock(Client::class);
        $mockClient->shouldReceive('select')->andReturn($returnData);

        $connection = app('db')->connection('clickhouse');
        $reflection = new \ReflectionClass($connection);
        $clientProperty = $reflection->getProperty('client');
        $clientProperty->setAccessible(true);
        $clientProperty->setValue($connection, $mockClient);
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
}
