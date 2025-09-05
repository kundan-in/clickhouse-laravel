<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use Illuminate\Database\DatabaseManager;
use KundanIn\ClickHouseLaravel\ClickHouseServiceProvider;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * ClickHouse Service Provider Test
 *
 * Tests the service provider registration and configuration publishing.
 */
class ClickHouseServiceProviderTest extends TestCase
{
    /**
     * Test that the service provider is registered correctly.
     *
     * @return void
     */
    public function test_service_provider_is_registered(): void
    {
        $this->assertTrue($this->app->providerIsLoaded(ClickHouseServiceProvider::class));
    }

    /**
     * Test that the configuration is merged correctly.
     *
     * @return void
     */
    public function test_configuration_is_merged(): void
    {
        $config = $this->app->get('config');

        $this->assertArrayHasKey('clickhouse', $config->all());
        $this->assertIsArray($config->get('clickhouse'));
        $this->assertEquals('127.0.0.1', $config->get('clickhouse.host'));
        $this->assertEquals(8123, $config->get('clickhouse.port'));
    }

    /**
     * Test that the ClickHouse database driver is extended.
     *
     * @return void
     */
    public function test_clickhouse_driver_is_extended(): void
    {
        $db = $this->app->get('db');

        $this->assertInstanceOf(DatabaseManager::class, $db);

        // Test that we can get the ClickHouse connection configuration
        $connection = $db->connection('clickhouse');
        $this->assertInstanceOf(ClickHouseConnection::class, $connection);
    }

    /**
     * Test that configuration publishing is available.
     *
     * @return void
     */
    public function test_configuration_publishing_is_available(): void
    {
        $provider = new ClickHouseServiceProvider($this->app);

        // Get the published paths
        $publishedPaths = $provider->publishedPaths();

        // Check if config publishing is registered
        $this->assertArrayHasKey('clickhouse-config', $publishedPaths);
    }
}
