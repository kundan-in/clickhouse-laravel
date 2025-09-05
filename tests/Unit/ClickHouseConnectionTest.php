<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use ClickHouseDB\Client;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use Mockery;

/**
 * ClickHouse Connection Test
 *
 * Tests the ClickHouse database connection functionality.
 *
 * @package KundanIn\ClickHouseLaravel\Tests\Unit
 */
class ClickHouseConnectionTest extends TestCase
{
    /**
     * Test ClickHouse connection instantiation.
     *
     * @return void
     */
    public function test_connection_instantiation(): void
    {
        $config = [
            'host' => '127.0.0.1',
            'port' => 8123,
            'username' => 'default',
            'password' => '',
            'database' => 'test',
        ];

        $connection = new ClickHouseConnection($config);
        
        $this->assertInstanceOf(ClickHouseConnection::class, $connection);
        $this->assertEquals($config, $connection->getConfig());
    }

    /**
     * Test select method calls the client.
     *
     * @return void
     */
    public function test_select_calls_client(): void
    {
        $mockClient = Mockery::mock(Client::class);
        $mockClient->shouldReceive('select')
            ->once()
            ->with('SELECT * FROM test', [])
            ->andReturn([['id' => 1, 'name' => 'test']]);

        $connection = $this->createConnectionWithMockClient($mockClient);
        
        $result = $connection->select('SELECT * FROM test');
        
        $this->assertEquals([['id' => 1, 'name' => 'test']], $result);
    }

    /**
     * Test insert method calls the client.
     *
     * @return void
     */
    public function test_insert_calls_client(): void
    {
        $mockClient = Mockery::mock(Client::class);
        $mockClient->shouldReceive('write')
            ->once()
            ->with('INSERT INTO test (name) VALUES (?)', ['test'])
            ->andReturn(true);

        $connection = $this->createConnectionWithMockClient($mockClient);
        
        $result = $connection->insert('INSERT INTO test (name) VALUES (?)', ['test']);
        
        $this->assertTrue($result);
    }

    /**
     * Test statement method calls the client.
     *
     * @return void
     */
    public function test_statement_calls_client(): void
    {
        $mockClient = Mockery::mock(Client::class);
        $mockClient->shouldReceive('write')
            ->once()
            ->with('CREATE TABLE test (id UInt32) ENGINE = Memory', [])
            ->andReturn(true);

        $connection = $this->createConnectionWithMockClient($mockClient);
        
        $result = $connection->statement('CREATE TABLE test (id UInt32) ENGINE = Memory');
        
        $this->assertTrue($result);
    }

    /**
     * Create a connection instance with a mock client.
     *
     * @param \ClickHouseDB\Client $mockClient
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseConnection
     */
    private function createConnectionWithMockClient(Client $mockClient): ClickHouseConnection
    {
        $config = [
            'host' => '127.0.0.1',
            'port' => 8123,
            'username' => 'default',
            'password' => '',
            'database' => 'test',
        ];

        $connection = new ClickHouseConnection($config);
        
        // Use reflection to inject the mock client
        $reflection = new \ReflectionClass($connection);
        $clientProperty = $reflection->getProperty('client');
        $clientProperty->setAccessible(true);
        $clientProperty->setValue($connection, $mockClient);

        return $connection;
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