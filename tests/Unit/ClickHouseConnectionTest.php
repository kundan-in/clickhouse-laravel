<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use ClickHouseDB\Client;
use ClickHouseDB\Statement;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use Mockery;

/**
 * ClickHouse Connection Test
 *
 * Tests the ClickHouse connection functionality and parameter binding.
 */
class ClickHouseConnectionTest extends TestCase
{
    protected ClickHouseConnection $connection;

    protected $mockClient;

    /**
     * Set up the test environment.
     *
     * @return void
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->mockClient = Mockery::mock(Client::class);

        $config = [
            'host' => '127.0.0.1',
            'port' => 8123,
            'username' => 'default',
            'password' => '',
            'database' => 'test_db',
        ];

        $this->connection = new ClickHouseConnection($config);

        // Use reflection to inject the mock client
        $reflection = new \ReflectionClass($this->connection);
        $clientProperty = $reflection->getProperty('client');
        $clientProperty->setAccessible(true);
        $clientProperty->setValue($this->connection, $this->mockClient);
    }

    /**
     * Test select method substitutes parameter bindings.
     *
     * @return void
     */
    public function test_select_substitutes_parameter_bindings(): void
    {
        $mockStatement = Mockery::mock(Statement::class);
        $mockStatement->shouldReceive('rows')
            ->andReturn([['id' => 1, 'session_id' => 'abc123']]);

        $this->mockClient->shouldReceive('select')
            ->with("select * from \"test_db\".\"web_visitor_events\" where \"session_id\" = 'abc123' LIMIT 1")
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->select(
            'select * from "test_db"."web_visitor_events" where "session_id" = ? LIMIT 1',
            ['abc123']
        );

        $this->assertEquals([['id' => 1, 'session_id' => 'abc123']], $result);
    }

    /**
     * Test select method handles numeric parameter bindings.
     *
     * @return void
     */
    public function test_select_handles_numeric_parameter_bindings(): void
    {
        $mockStatement = Mockery::mock(Statement::class);
        $mockStatement->shouldReceive('rows')
            ->andReturn([['id' => 1, 'user_id' => 123]]);

        $this->mockClient->shouldReceive('select')
            ->with('select * from "test_db"."events" where "user_id" = 123 AND "active" = 1')
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->select(
            'select * from "test_db"."events" where "user_id" = ? AND "active" = ?',
            [123, true]
        );

        $this->assertEquals([['id' => 1, 'user_id' => 123]], $result);
    }

    /**
     * Test select method handles NULL parameter bindings.
     *
     * @return void
     */
    public function test_select_handles_null_parameter_bindings(): void
    {
        $mockStatement = Mockery::mock(Statement::class);
        $mockStatement->shouldReceive('rows')
            ->andReturn([['id' => 1, 'deleted_at' => null]]);

        $this->mockClient->shouldReceive('select')
            ->with('select * from "test_db"."events" where "deleted_at" = NULL')
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->select(
            'select * from "test_db"."events" where "deleted_at" = ?',
            [null]
        );

        $this->assertEquals([['id' => 1, 'deleted_at' => null]], $result);
    }

    /**
     * Test select method escapes string quotes properly.
     *
     * @return void
     */
    public function test_select_escapes_string_quotes(): void
    {
        $mockStatement = Mockery::mock(Statement::class);
        $mockStatement->shouldReceive('rows')
            ->andReturn([['id' => 1, 'name' => "John's data"]]);

        $this->mockClient->shouldReceive('select')
            ->with("select * from \"test_db\".\"users\" where \"name\" = 'John\\'s data'")
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->select(
            'select * from "test_db"."users" where "name" = ?',
            ["John's data"]
        );

        $this->assertEquals([['id' => 1, 'name' => "John's data"]], $result);
    }

    /**
     * Test select method works without parameter bindings.
     *
     * @return void
     */
    public function test_select_works_without_parameter_bindings(): void
    {
        $mockStatement = Mockery::mock(Statement::class);
        $mockStatement->shouldReceive('rows')
            ->andReturn([['count' => 5]]);

        $this->mockClient->shouldReceive('select')
            ->with('select count() from "test_db"."events"')
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->select('select count() from "test_db"."events"');

        $this->assertEquals([['count' => 5]], $result);
    }

    /**
     * Test selectOne method returns single record.
     *
     * @return void
     */
    public function test_select_one_returns_single_record(): void
    {
        $mockStatement = Mockery::mock(Statement::class);
        $mockStatement->shouldReceive('rows')
            ->andReturn([['id' => 1, 'session_id' => 'abc123']]);

        $this->mockClient->shouldReceive('select')
            ->with("select * from \"test_db\".\"events\" where \"session_id\" = 'abc123' LIMIT 1")
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->selectOne(
            'select * from "test_db"."events" where "session_id" = ? LIMIT 1',
            ['abc123']
        );

        $this->assertEquals(['id' => 1, 'session_id' => 'abc123'], $result);
    }

    /**
     * Test scalar method returns single value.
     *
     * @return void
     */
    public function test_scalar_returns_single_value(): void
    {
        $mockStatement = Mockery::mock(Statement::class);
        $mockStatement->shouldReceive('rows')
            ->andReturn([['count' => 42]]);

        $this->mockClient->shouldReceive('select')
            ->with("select count() from \"test_db\".\"events\" where \"status\" = 'active'")
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->scalar(
            'select count() from "test_db"."events" where "status" = ?',
            ['active']
        );

        $this->assertEquals(42, $result);
    }

    /**
     * Clean up Mockery after each test.
     *
     * @return void
     */
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
}
