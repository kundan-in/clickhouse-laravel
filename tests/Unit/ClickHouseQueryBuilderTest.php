<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use ClickHouseDB\Client;
use ClickHouseDB\Statement;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use Mockery;

/**
 * Tests for the ClickHouse query builder SQL generation and execution.
 */
class ClickHouseQueryBuilderTest extends TestCase
{
    protected ClickHouseConnection $connection;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->createTestConnection();
    }

    // ---------------------------------------------------------------
    // SQL generation tests (case-insensitive assertions)
    // ---------------------------------------------------------------

    public function test_basic_select_query(): void
    {
        $sql = $this->connection->table('events')->toSql();

        $this->assertStringContainsStringIgnoringCase('select * from', $sql);
        $this->assertStringContainsString('"events"', $sql);
    }

    public function test_select_specific_columns(): void
    {
        $sql = $this->connection->table('events')
            ->select(['id', 'name', 'created_at'])
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('select', $sql);
        $this->assertStringContainsString('"id"', $sql);
        $this->assertStringContainsString('"name"', $sql);
        $this->assertStringContainsString('"created_at"', $sql);
    }

    public function test_where_clause(): void
    {
        $sql = $this->connection->table('events')
            ->where('status', '=', 'active')
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('where', $sql);
        $this->assertStringContainsString('"status" = ?', $sql);
    }

    public function test_multiple_where_clauses(): void
    {
        $sql = $this->connection->table('events')
            ->where('status', 'active')
            ->where('type', 'click')
            ->toSql();

        $this->assertStringContainsString('"status" = ?', $sql);
        $this->assertStringContainsStringIgnoringCase('and', $sql);
        $this->assertStringContainsString('"type" = ?', $sql);
    }

    public function test_where_in_clause(): void
    {
        $sql = $this->connection->table('events')
            ->whereIn('status', ['active', 'pending'])
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('where', $sql);
        $this->assertStringContainsString('"status" in (?, ?)', $sql);
    }

    public function test_where_between_clause(): void
    {
        $sql = $this->connection->table('events')
            ->whereBetween('created_at', ['2023-01-01', '2023-12-31'])
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('between', $sql);
        $this->assertStringContainsString('"created_at"', $sql);
    }

    public function test_where_date_uses_clickhouse_function(): void
    {
        $sql = $this->connection->table('events')
            ->whereDate('created_at', '2023-09-05')
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('toDate(', $sql);
        $this->assertStringContainsString('"created_at"', $sql);
    }

    public function test_order_by_clause(): void
    {
        $sql = $this->connection->table('events')
            ->orderBy('created_at', 'desc')
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('order by', $sql);
        $this->assertStringContainsString('"created_at"', $sql);
        $this->assertStringContainsStringIgnoringCase('desc', $sql);
    }

    public function test_limit_clause(): void
    {
        $sql = $this->connection->table('events')
            ->limit(100)
            ->toSql();

        $this->assertStringContainsString('LIMIT 100', $sql);
    }

    public function test_group_by_clause(): void
    {
        $sql = $this->connection->table('events')
            ->select(['status', $this->connection->raw('COUNT(*) as count')])
            ->groupBy('status')
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('group by', $sql);
        $this->assertStringContainsString('"status"', $sql);
    }

    public function test_distinct_clause(): void
    {
        $sql = $this->connection->table('events')
            ->distinct()
            ->select('status')
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('select distinct', $sql);
    }

    public function test_join_operations(): void
    {
        $sql = $this->connection->table('events')
            ->join('users', 'events.user_id', '=', 'users.id')
            ->select(['events.*', 'users.name'])
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('inner join', $sql);
        $this->assertStringContainsString('"users"', $sql);
    }

    public function test_left_join_operations(): void
    {
        $sql = $this->connection->table('events')
            ->leftJoin('users', 'events.user_id', '=', 'users.id')
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('left join', $sql);
    }

    // ---------------------------------------------------------------
    // Execution tests (with properly mocked Statement)
    // ---------------------------------------------------------------

    public function test_count_aggregation(): void
    {
        $connection = $this->createConnectionWithMockResult([['aggregate' => 150]]);

        $result = $connection->table('events')->count();

        $this->assertEquals(150, $result);
    }

    public function test_max_aggregation(): void
    {
        $connection = $this->createConnectionWithMockResult([['aggregate' => 1000]]);

        $result = $connection->table('events')->max('id');

        $this->assertEquals(1000, $result);
    }

    public function test_min_aggregation(): void
    {
        $connection = $this->createConnectionWithMockResult([['aggregate' => 1]]);

        $result = $connection->table('events')->min('id');

        $this->assertEquals(1, $result);
    }

    public function test_sum_aggregation(): void
    {
        $connection = $this->createConnectionWithMockResult([['aggregate' => 12500]]);

        $result = $connection->table('events')->sum('amount');

        $this->assertEquals(12500, $result);
    }

    public function test_first_method(): void
    {
        $expected = ['id' => 1, 'name' => 'Test Event'];
        $connection = $this->createConnectionWithMockResult([$expected]);

        $result = $connection->table('events')->first();

        $this->assertEquals($expected, $result);
    }

    public function test_get_method(): void
    {
        $data = [
            ['id' => 1, 'name' => 'Event 1'],
            ['id' => 2, 'name' => 'Event 2'],
        ];
        $connection = $this->createConnectionWithMockResult($data);

        $results = $connection->table('events')->get();

        $this->assertCount(2, $results);
    }

    public function test_pluck_method(): void
    {
        $data = [
            ['name' => 'Event 1'],
            ['name' => 'Event 2'],
        ];
        $connection = $this->createConnectionWithMockResult($data);

        $results = $connection->table('events')->pluck('name');

        $this->assertEquals(['Event 1', 'Event 2'], $results->toArray());
    }

    public function test_exists_method(): void
    {
        $connection = $this->createConnectionWithMockResult([['exists' => 1]]);

        $result = $connection->table('events')->where('id', 1)->exists();

        $this->assertTrue($result);
    }

    public function test_doesnt_exist_method(): void
    {
        $connection = $this->createConnectionWithMockResult([]);

        $result = $connection->table('events')->where('id', 999)->doesntExist();

        $this->assertTrue($result);
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private function createTestConnection(): ClickHouseConnection
    {
        return new ClickHouseConnection([
            'host' => '127.0.0.1',
            'port' => 8123,
            'database' => 'test_database',
            'username' => 'default',
            'password' => '',
        ]);
    }

    private function createConnectionWithMockResult(array $rows): ClickHouseConnection
    {
        $statement = Mockery::mock(Statement::class);
        $statement->shouldReceive('rows')->andReturn($rows);

        $mockClient = Mockery::mock(Client::class);
        $mockClient->shouldReceive('select')->andReturn($statement);

        $connection = $this->createTestConnection();

        $reflection = new \ReflectionClass($connection);
        $prop = $reflection->getProperty('client');
        $prop->setAccessible(true);
        $prop->setValue($connection, $mockClient);

        return $connection;
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
}
