<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use ClickHouseDB\Client;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use Mockery;

/**
 * ClickHouse Specific Operations Test
 *
 * Tests for ClickHouse-specific database operations that differ from standard SQL.
 * Includes ALTER TABLE operations, bulk inserts, and other ClickHouse features.
 */
class ClickHouseSpecificOperationsTest extends TestCase
{
    protected ClickHouseConnection $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->createMockConnection();
    }

    /**
     * Test ALTER TABLE UPDATE operation.
     */
    public function test_alter_table_update_operation(): void
    {
        $mockClient = $this->createMockClientForWrite();
        $connection = $this->createConnectionWithMockClient($mockClient);

        $result = $connection->table('web_visitor_events')
            ->where('visitor_id', 'vis_123')
            ->update(['event_type' => 'updated_click']);

        $this->assertEquals(1, $result);
    }

    /**
     * Test ALTER TABLE DELETE operation.
     */
    public function test_alter_table_delete_operation(): void
    {
        $mockClient = $this->createMockClientForWrite();
        $connection = $this->createConnectionWithMockClient($mockClient);

        $result = $connection->table('web_visitor_events')
            ->where('created_at', '<', '2023-01-01')
            ->delete();

        $this->assertEquals(1, $result);
    }

    /**
     * Test bulk insert operation.
     */
    public function test_bulk_insert_operation(): void
    {
        $mockClient = $this->createMockClientForWrite(true);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $data = [
            ['visitor_id' => 'vis_123', 'event_type' => 'page_view', 'url' => '/home'],
            ['visitor_id' => 'vis_124', 'event_type' => 'click', 'url' => '/products'],
            ['visitor_id' => 'vis_125', 'event_type' => 'scroll', 'url' => '/about'],
        ];

        $result = $connection->table('web_visitor_events')->insert($data);

        $this->assertTrue($result);
    }

    /**
     * Test raw SQL execution.
     */
    public function test_raw_sql_execution(): void
    {
        $mockData = [['count' => 500, 'event_type' => 'page_view']];
        $mockClient = $this->createMockClient($mockData);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $results = $connection->select("
            SELECT event_type, count(*) as count 
            FROM web_visitor_events 
            WHERE created_at >= '2023-01-01' 
            GROUP BY event_type 
            ORDER BY count DESC
        ");

        $this->assertCount(1, $results);
        $this->assertEquals(500, $results[0]['count']);
        $this->assertEquals('page_view', $results[0]['event_type']);
    }

    /**
     * Test ClickHouse-specific functions with raw expressions.
     */
    public function test_clickhouse_specific_functions(): void
    {
        $sql = $this->connection->table('web_visitor_events')
            ->select([
                'visitor_id',
                $this->connection->raw('toDate(created_at) as date'),
                $this->connection->raw('toHour(created_at) as hour'),
                $this->connection->raw('JSONExtractString(metadata, \'browser\') as browser'),
            ])
            ->toSql();

        $this->assertStringContainsString('toDate(created_at) as date', $sql);
        $this->assertStringContainsString('toHour(created_at) as hour', $sql);
        $this->assertStringContainsString('JSONExtractString', $sql);
    }

    /**
     * Test window functions.
     */
    public function test_window_functions(): void
    {
        $sql = $this->connection->table('web_visitor_events')
            ->select([
                'visitor_id',
                'event_type',
                'created_at',
                $this->connection->raw('ROW_NUMBER() OVER (PARTITION BY visitor_id ORDER BY created_at) as event_sequence'),
            ])
            ->toSql();

        $this->assertStringContainsString('ROW_NUMBER() OVER', $sql);
        $this->assertStringContainsString('PARTITION BY', $sql);
    }

    /**
     * Test array operations.
     */
    public function test_array_operations(): void
    {
        $sql = $this->connection->table('web_visitor_events')
            ->select([
                'visitor_id',
                $this->connection->raw('groupArray(event_type) as event_types'),
                $this->connection->raw('arrayJoin([1,2,3]) as numbers'),
            ])
            ->groupBy('visitor_id')
            ->toSql();

        $this->assertStringContainsString('groupArray(event_type)', $sql);
        $this->assertStringContainsString('arrayJoin', $sql);
    }

    /**
     * Test time-based partitioning queries.
     */
    public function test_time_based_partitioning_queries(): void
    {
        $sql = $this->connection->table('web_visitor_events')
            ->select([
                $this->connection->raw('toYYYYMM(created_at) as month_partition'),
                $this->connection->raw('count(*) as event_count'),
            ])
            ->groupBy($this->connection->raw('toYYYYMM(created_at)'))
            ->orderBy($this->connection->raw('toYYYYMM(created_at)'))
            ->toSql();

        $this->assertStringContainsString('toYYYYMM(created_at)', $sql);
        $this->assertStringContainsString('group by toYYYYMM(created_at)', $sql);
    }

    /**
     * Test materialized view-style aggregations.
     */
    public function test_materialized_view_aggregations(): void
    {
        $sql = $this->connection->table('web_visitor_events')
            ->select([
                'event_type',
                $this->connection->raw('toDate(created_at) as date'),
                $this->connection->raw('count(*) as total_events'),
                $this->connection->raw('uniq(visitor_id) as unique_visitors'),
                $this->connection->raw('avg(session_duration) as avg_session_duration'),
            ])
            ->groupBy(['event_type', $this->connection->raw('toDate(created_at)')])
            ->toSql();

        $this->assertStringContainsString('uniq(visitor_id)', $sql);
        $this->assertStringContainsString('count(*) as total_events', $sql);
    }

    /**
     * Test WITH TOTALS clause.
     */
    public function test_with_totals_clause(): void
    {
        $mockData = [
            ['event_type' => 'page_view', 'count' => 150],
            ['event_type' => 'click', 'count' => 75],
            ['event_type' => '', 'count' => 225], // TOTALS row
        ];

        $mockClient = $this->createMockClient($mockData);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $results = $connection->select('
            SELECT event_type, count(*) as count 
            FROM web_visitor_events 
            GROUP BY event_type 
            WITH TOTALS
        ');

        $this->assertCount(3, $results);
        $this->assertEquals(225, $results[2]['count']); // TOTALS row
    }

    /**
     * Test approximate aggregation functions.
     */
    public function test_approximate_aggregation_functions(): void
    {
        $sql = $this->connection->table('web_visitor_events')
            ->select([
                $this->connection->raw('approxCountDistinct(visitor_id) as approx_unique_visitors'),
                $this->connection->raw('quantile(0.5)(session_duration) as median_session_duration'),
                $this->connection->raw('quantile(0.95)(page_load_time) as p95_load_time'),
            ])
            ->toSql();

        $this->assertStringContainsString('approxCountDistinct', $sql);
        $this->assertStringContainsString('quantile(0.5)', $sql);
        $this->assertStringContainsString('quantile(0.95)', $sql);
    }

    /**
     * Test SAMPLE clause for performance on large datasets.
     */
    public function test_sample_clause(): void
    {
        $mockData = [['avg_duration' => 45.2]];
        $mockClient = $this->createMockClient($mockData);
        $connection = $this->createConnectionWithMockClient($mockClient);

        $results = $connection->select('
            SELECT avg(session_duration) as avg_duration 
            FROM web_visitor_events 
            SAMPLE 0.1
        ');

        $this->assertEquals(45.2, $results[0]['avg_duration']);
    }

    /**
     * Test database.table notation in queries.
     */
    public function test_database_table_notation(): void
    {
        $sql = $this->connection->table('web_visitor_events')
            ->join('users', 'web_visitor_events.user_id', '=', 'users.id')
            ->select(['web_visitor_events.*', 'users.name'])
            ->toSql();

        $this->assertStringContainsString('"web_visitor_events"', $sql);
        $this->assertStringContainsString('"users"', $sql);
        $this->assertStringContainsString('inner join', $sql);
    }

    /**
     * Test complex analytical queries.
     */
    public function test_complex_analytical_queries(): void
    {
        $sql = $this->connection->table('web_visitor_events')
            ->select([
                'visitor_id',
                $this->connection->raw('count(*) as total_events'),
                $this->connection->raw('min(created_at) as first_event'),
                $this->connection->raw('max(created_at) as last_event'),
                $this->connection->raw('dateDiff(\'minute\', min(created_at), max(created_at)) as session_duration_minutes'),
                $this->connection->raw('arrayStringConcat(groupArray(event_type), \',\') as event_sequence'),
            ])
            ->groupBy('visitor_id')
            ->having($this->connection->raw('count(*)'), '>', 5)
            ->orderBy($this->connection->raw('count(*)'), 'desc')
            ->toSql();

        $this->assertStringContainsString('dateDiff', $sql);
        $this->assertStringContainsString('arrayStringConcat', $sql);
        $this->assertStringContainsString('groupArray', $sql);
        $this->assertStringContainsString('having count(*) > ?', $sql);
    }

    /**
     * Test exception handling for unsupported operations.
     */
    public function test_exception_for_update_without_where(): void
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('UPDATE queries on ClickHouse require a WHERE clause for safety.');

        $mockClient = $this->createMockClientForWrite();
        $connection = $this->createConnectionWithMockClient($mockClient);

        $connection->table('web_visitor_events')->update(['status' => 'processed']);
    }

    /**
     * Test exception handling for delete without where.
     */
    public function test_exception_for_delete_without_where(): void
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('DELETE queries on ClickHouse require a WHERE clause for safety.');

        $mockClient = $this->createMockClientForWrite();
        $connection = $this->createConnectionWithMockClient($mockClient);

        $connection->table('web_visitor_events')->delete();
    }

    /**
     * Test connection configuration and client setup.
     */
    public function test_connection_configuration(): void
    {
        $config = [
            'host' => '127.0.0.1',
            'port' => 8123,
            'database' => 'test_database',
            'username' => 'default',
            'password' => 'secret',
            'settings' => [
                'readonly' => 0,
                'max_execution_time' => 60,
            ],
        ];

        $connection = new ClickHouseConnection($config);

        $this->assertInstanceOf(ClickHouseConnection::class, $connection);
        $this->assertEquals('test_database', $connection->getDatabaseName());
        $this->assertEquals($config, $connection->getConfig());
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
     * Create a mock ClickHouse client for read operations.
     */
    private function createMockClient(array $returnData = []): Client
    {
        $mockClient = Mockery::mock(Client::class);
        $mockClient->shouldReceive('select')->andReturn($returnData);

        return $mockClient;
    }

    /**
     * Create a mock ClickHouse client for write operations.
     */
    private function createMockClientForWrite(bool $returnValue = true): Client
    {
        $mockClient = Mockery::mock(Client::class);
        $mockClient->shouldReceive('write')->andReturn($returnValue);

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
