<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use ClickHouseDB\Client;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use Mockery;

/**
 * ClickHouse Query Builder Test
 *
 * Comprehensive tests for Laravel query builder operations with ClickHouse.
 * Tests all major query builder methods to ensure compatibility.
 */
class ClickHouseQueryBuilderTest extends TestCase
{
    protected ClickHouseConnection $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->createMockConnection();
    }

    /**
     * Test basic select query.
     */
    public function test_basic_select_query(): void
    {
        $result = $this->connection->table('events')->toSql();

        $this->assertStringContainsString('select * from "events"', $result);
    }

    /**
     * Test select with specific columns.
     */
    public function test_select_specific_columns(): void
    {
        $result = $this->connection->table('events')
            ->select(['id', 'name', 'created_at'])
            ->toSql();

        $this->assertStringContainsString('select "id", "name", "created_at"', $result);
        $this->assertStringContainsString('"events"', $result);
    }

    /**
     * Test where clause.
     */
    public function test_where_clause(): void
    {
        $result = $this->connection->table('events')
            ->where('status', '=', 'active')
            ->toSql();

        $this->assertStringContainsString('where "status" = ?', $result);
    }

    /**
     * Test multiple where clauses.
     */
    public function test_multiple_where_clauses(): void
    {
        $result = $this->connection->table('events')
            ->where('status', 'active')
            ->where('type', 'click')
            ->toSql();

        $this->assertStringContainsString('where "status" = ? and "type" = ?', $result);
    }

    /**
     * Test whereIn clause.
     */
    public function test_where_in_clause(): void
    {
        $result = $this->connection->table('events')
            ->whereIn('status', ['active', 'pending'])
            ->toSql();

        $this->assertStringContainsString('where "status" in (?, ?)', $result);
    }

    /**
     * Test whereBetween clause.
     */
    public function test_where_between_clause(): void
    {
        $result = $this->connection->table('events')
            ->whereBetween('created_at', ['2023-01-01', '2023-12-31'])
            ->toSql();

        $this->assertStringContainsString('where "created_at" between ? and ?', $result);
    }

    /**
     * Test whereDate clause.
     */
    public function test_where_date_clause(): void
    {
        $result = $this->connection->table('events')
            ->whereDate('created_at', '2023-09-05')
            ->toSql();

        $this->assertStringContainsString('where date("created_at") = ?', $result);
    }

    /**
     * Test orderBy clause.
     */
    public function test_order_by_clause(): void
    {
        $result = $this->connection->table('events')
            ->orderBy('created_at', 'desc')
            ->toSql();

        $this->assertStringContainsString('order by "created_at" desc', $result);
    }

    /**
     * Test multiple orderBy clauses.
     */
    public function test_multiple_order_by_clauses(): void
    {
        $result = $this->connection->table('events')
            ->orderBy('status', 'asc')
            ->orderBy('created_at', 'desc')
            ->toSql();

        $this->assertStringContainsString('order by "status" asc, "created_at" desc', $result);
    }

    /**
     * Test limit clause.
     */
    public function test_limit_clause(): void
    {
        $result = $this->connection->table('events')
            ->limit(100)
            ->toSql();

        $this->assertStringContainsString('LIMIT 100', $result);
    }

    /**
     * Test offset clause.
     */
    public function test_offset_clause(): void
    {
        $result = $this->connection->table('events')
            ->offset(50)
            ->limit(100)
            ->toSql();

        $this->assertStringContainsString('LIMIT 100 offset 50', $result);
    }

    /**
     * Test groupBy clause.
     */
    public function test_group_by_clause(): void
    {
        $result = $this->connection->table('events')
            ->select(['status', $this->connection->raw('COUNT(*) as count')])
            ->groupBy('status')
            ->toSql();

        $this->assertStringContainsString('group by "status"', $result);
    }

    /**
     * Test having clause.
     */
    public function test_having_clause(): void
    {
        $result = $this->connection->table('events')
            ->select(['status', $this->connection->raw('COUNT(*) as count')])
            ->groupBy('status')
            ->having('count', '>', 10)
            ->toSql();

        $this->assertStringContainsString('having "count" > ?', $result);
    }

    /**
     * Test distinct clause.
     */
    public function test_distinct_clause(): void
    {
        $result = $this->connection->table('events')
            ->distinct()
            ->select('status')
            ->toSql();

        $this->assertStringContainsString('select distinct "status"', $result);
    }

    /**
     * Test join operations.
     */
    public function test_join_operations(): void
    {
        $result = $this->connection->table('events')
            ->join('users', 'events.user_id', '=', 'users.id')
            ->select(['events.*', 'users.name'])
            ->toSql();

        $this->assertStringContainsString('inner join "users"', $result);
        $this->assertStringContainsString('on "events"."user_id" = "users"."id"', $result);
    }

    /**
     * Test left join operations.
     */
    public function test_left_join_operations(): void
    {
        $result = $this->connection->table('events')
            ->leftJoin('users', 'events.user_id', '=', 'users.id')
            ->toSql();

        $this->assertStringContainsString('left join "users"', $result);
    }

    /**
     * Test count aggregation.
     */
    public function test_count_aggregation(): void
    {
        $mockClient = $this->createMockClient([['aggregate' => 150]]);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $result = $connection->table('events')->count();

        $this->assertEquals(150, $result);
    }

    /**
     * Test max aggregation.
     */
    public function test_max_aggregation(): void
    {
        $mockClient = $this->createMockClient([['aggregate' => 1000]]);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $result = $connection->table('events')->max('id');

        $this->assertEquals(1000, $result);
    }

    /**
     * Test min aggregation.
     */
    public function test_min_aggregation(): void
    {
        $mockClient = $this->createMockClient([['aggregate' => 1]]);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $result = $connection->table('events')->min('id');

        $this->assertEquals(1, $result);
    }

    /**
     * Test avg aggregation.
     */
    public function test_avg_aggregation(): void
    {
        $mockClient = $this->createMockClient([['aggregate' => 42.5]]);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $result = $connection->table('events')->avg('score');

        $this->assertEquals(42.5, $result);
    }

    /**
     * Test sum aggregation.
     */
    public function test_sum_aggregation(): void
    {
        $mockClient = $this->createMockClient([['aggregate' => 12500]]);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $result = $connection->table('events')->sum('amount');

        $this->assertEquals(12500, $result);
    }

    /**
     * Test first() method.
     */
    public function test_first_method(): void
    {
        $expectedData = ['id' => 1, 'name' => 'Test Event', 'status' => 'active'];
        $mockClient = $this->createMockClient([$expectedData]);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $result = $connection->table('events')->first();

        $this->assertEquals($expectedData, $result);
    }

    /**
     * Test get() method.
     */
    public function test_get_method(): void
    {
        $expectedData = [
            ['id' => 1, 'name' => 'Event 1'],
            ['id' => 2, 'name' => 'Event 2'],
        ];
        $mockClient = $this->createMockClient($expectedData);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $results = $connection->table('events')->get();

        $this->assertCount(2, $results);
        $this->assertEquals($expectedData[0], $results[0]);
        $this->assertEquals($expectedData[1], $results[1]);
    }

    /**
     * Test pluck() method.
     */
    public function test_pluck_method(): void
    {
        $mockData = [
            ['id' => 1, 'name' => 'Event 1'],
            ['id' => 2, 'name' => 'Event 2'],
        ];
        $mockClient = $this->createMockClient($mockData);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $results = $connection->table('events')->pluck('name');

        $this->assertEquals(['Event 1', 'Event 2'], $results->toArray());
    }

    /**
     * Test chunk() method.
     */
    public function test_chunk_method(): void
    {
        $mockData = [
            ['id' => 1, 'name' => 'Event 1'],
            ['id' => 2, 'name' => 'Event 2'],
        ];
        $mockClient = $this->createMockClient($mockData);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $processedRecords = [];
        $connection->table('events')->orderBy('id')->chunk(100, function ($records) use (&$processedRecords) {
            foreach ($records as $record) {
                $processedRecords[] = $record;
            }
        });

        $this->assertCount(2, $processedRecords);
        $this->assertEquals($mockData[0], $processedRecords[0]);
    }

    /**
     * Test exists() method.
     */
    public function test_exists_method(): void
    {
        $mockClient = $this->createMockClient([['exists' => 1]]);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $result = $connection->table('events')->where('id', 1)->exists();

        $this->assertTrue($result);
    }

    /**
     * Test doesntExist() method.
     */
    public function test_doesnt_exist_method(): void
    {
        $mockClient = $this->createMockClient([]);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $result = $connection->table('events')->where('id', 999)->doesntExist();

        $this->assertTrue($result);
    }

    /**
     * Create a mock ClickHouse connection.
     */
    private function createMockConnection(): ClickHouseConnection
    {
        $config = [
            'host' => '127.0.0.1',
            'port' => 8123,
            'database' => 'test_database',
            'username' => 'default',
            'password' => '',
        ];

        return new ClickHouseConnection($config);
    }

    /**
     * Create a mock ClickHouse client.
     */
    private function createMockClient(array $returnData = []): Client
    {
        $mockClient = Mockery::mock(Client::class);
        $mockClient->shouldReceive('select')
            ->andReturn($returnData);

        return $mockClient;
    }

    /**
     * Create connection with mock client.
     */
    private function createConnectionWithMockClient(Client $mockClient): ClickHouseConnection
    {
        $connection = $this->createMockConnection();

        $reflection = new \ReflectionClass($connection);
        $clientProperty = $reflection->getProperty('client');
        $clientProperty->setAccessible(true);
        $clientProperty->setValue($connection, $mockClient);

        return $connection;
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
}
