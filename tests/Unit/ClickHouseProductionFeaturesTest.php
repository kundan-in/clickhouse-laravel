<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use ClickHouseDB\Client;
use ClickHouseDB\Statement;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Exceptions\UnsupportedOperationException;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use Mockery;

/**
 * Tests for production features: bulkInsert, cursor, insertGetId, upsert.
 */
class ClickHouseProductionFeaturesTest extends TestCase
{
    private function makeConnection(): ClickHouseConnection
    {
        return new ClickHouseConnection([
            'host' => '127.0.0.1',
            'port' => 8123,
            'username' => 'default',
            'password' => '',
            'database' => 'test_db',
        ]);
    }

    private function makeConnectionWithMock(): array
    {
        $connection = $this->makeConnection();
        $mockClient = Mockery::mock(Client::class);

        $ref = new \ReflectionClass($connection);
        $prop = $ref->getProperty('client');
        $prop->setAccessible(true);
        $prop->setValue($connection, $mockClient);

        return [$connection, $mockClient];
    }

    // -----------------------------------------------------------------
    // bulkInsert
    // -----------------------------------------------------------------

    public function test_bulk_insert_calls_client_insert(): void
    {
        [$connection, $mockClient] = $this->makeConnectionWithMock();

        $rows = [
            ['id' => 1, 'name' => 'Alice'],
            ['id' => 2, 'name' => 'Bob'],
        ];

        $mockClient->shouldReceive('insert')
            ->with('events', $rows, ['id', 'name'])
            ->once();

        $result = $connection->bulkInsert('events', $rows);

        $this->assertTrue($result);
    }

    public function test_bulk_insert_with_empty_rows_returns_true(): void
    {
        $connection = $this->makeConnection();

        $result = $connection->bulkInsert('events', []);

        $this->assertTrue($result);
    }

    public function test_bulk_insert_auto_detects_columns(): void
    {
        [$connection, $mockClient] = $this->makeConnectionWithMock();

        $rows = [['user_id' => 1, 'action' => 'click']];

        $mockClient->shouldReceive('insert')
            ->with('events', $rows, ['user_id', 'action'])
            ->once();

        $result = $connection->bulkInsert('events', $rows);

        $this->assertTrue($result);
    }

    // -----------------------------------------------------------------
    // cursor
    // -----------------------------------------------------------------

    public function test_cursor_returns_generator(): void
    {
        [$connection, $mockClient] = $this->makeConnectionWithMock();

        $statement = Mockery::mock(Statement::class);
        $statement->shouldReceive('rows')->andReturn([
            ['id' => 1, 'name' => 'Alice'],
            ['id' => 2, 'name' => 'Bob'],
        ]);
        $mockClient->shouldReceive('select')->andReturn($statement);

        $results = [];
        foreach ($connection->cursor('SELECT * FROM events') as $row) {
            $results[] = $row;
        }

        $this->assertCount(2, $results);
        $this->assertIsObject($results[0]);
        $this->assertEquals('Alice', $results[0]->name);
    }

    // -----------------------------------------------------------------
    // insertGetId throws
    // -----------------------------------------------------------------

    public function test_insert_get_id_throws_exception(): void
    {
        $connection = $this->makeConnection();

        $this->expectException(UnsupportedOperationException::class);
        $this->expectExceptionMessage('auto-incrementing');

        $connection->table('events')->insertGetId(['name' => 'test']);
    }

    // -----------------------------------------------------------------
    // upsert throws
    // -----------------------------------------------------------------

    public function test_upsert_throws_exception(): void
    {
        $connection = $this->makeConnection();

        $this->expectException(UnsupportedOperationException::class);
        $this->expectExceptionMessage('ReplacingMergeTree');

        $connection->table('events')->upsert(
            [['id' => 1, 'name' => 'test']],
            ['id'],
            ['name']
        );
    }

    // -----------------------------------------------------------------
    // getDriverName
    // -----------------------------------------------------------------

    public function test_get_driver_name_returns_clickhouse(): void
    {
        $connection = $this->makeConnection();

        $this->assertEquals('clickhouse', $connection->getDriverName());
    }

    // -----------------------------------------------------------------
    // disconnect
    // -----------------------------------------------------------------

    public function test_disconnect_does_not_throw(): void
    {
        $connection = $this->makeConnection();
        $connection->disconnect();

        $this->assertTrue(true);
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
}
