<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use ClickHouseDB\Client;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnector;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * Tests for timeout configuration on the ClickHouse connection.
 */
class ClickHouseTimeoutConfigTest extends TestCase
{
    /**
     * Get the underlying ClickHouse client from a connection via reflection.
     */
    protected function getClient(ClickHouseConnection $connection): Client
    {
        $reflection = new \ReflectionClass($connection);
        $property = $reflection->getProperty('client');
        $property->setAccessible(true);

        return $property->getValue($connection);
    }

    /**
     * Build a minimal config array with the given overrides.
     *
     * @param  array  $overrides
     * @return array
     */
    protected function makeConfig(array $overrides = []): array
    {
        return array_merge([
            'host' => '127.0.0.1',
            'port' => 8123,
            'username' => 'default',
            'password' => '',
            'database' => 'test',
        ], $overrides);
    }

    public function test_connect_timeout_is_applied_to_client(): void
    {
        $connection = new ClickHouseConnection($this->makeConfig([
            'connect_timeout' => 10,
        ]));

        $client = $this->getClient($connection);

        $this->assertSame(10.0, $client->getConnectTimeOut());
    }

    public function test_timeout_is_applied_to_client(): void
    {
        $connection = new ClickHouseConnection($this->makeConfig([
            'timeout' => 120,
        ]));

        $client = $this->getClient($connection);

        $this->assertSame(120, $client->getTimeout());
    }

    public function test_default_connect_timeout_when_not_configured(): void
    {
        $connection = new ClickHouseConnection($this->makeConfig());

        $client = $this->getClient($connection);

        // smi2/phpClickHouse default is 5.0 seconds
        $this->assertSame(5.0, $client->getConnectTimeOut());
    }

    public function test_connect_timeout_accepts_float_values(): void
    {
        $connection = new ClickHouseConnection($this->makeConfig([
            'connect_timeout' => 2.5,
        ]));

        $client = $this->getClient($connection);

        $this->assertSame(2.5, $client->getConnectTimeOut());
    }

    public function test_both_timeouts_can_be_set_together(): void
    {
        $connection = new ClickHouseConnection($this->makeConfig([
            'timeout' => 300,
            'connect_timeout' => 15,
        ]));

        $client = $this->getClient($connection);

        $this->assertSame(300, $client->getTimeout());
        $this->assertSame(15.0, $client->getConnectTimeOut());
    }

    public function test_connector_applies_timeout_settings(): void
    {
        $connector = new ClickHouseConnector;

        $config = $this->makeConfig([
            'timeout' => 90,
            'connect_timeout' => 8,
        ]);

        $client = $connector->connect($config);

        $this->assertInstanceOf(Client::class, $client);
        $this->assertSame(90, $client->getTimeout());
        $this->assertSame(8.0, $client->getConnectTimeOut());
    }

    public function test_zero_timeout_is_applied(): void
    {
        $connection = new ClickHouseConnection($this->makeConfig([
            'timeout' => 0,
        ]));

        $client = $this->getClient($connection);

        $this->assertSame(0, $client->getTimeout());
    }
}
