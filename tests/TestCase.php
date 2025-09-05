<?php

namespace KundanIn\ClickHouseLaravel\Tests;

use KundanIn\ClickHouseLaravel\ClickHouseServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

/**
 * Base Test Case
 *
 * This class provides the base functionality for all package tests,
 * setting up the Laravel testing environment with the ClickHouse service provider.
 */
abstract class TestCase extends Orchestra
{
    /**
     * Set up the test environment.
     *
     * @return void
     */
    protected function setUp(): void
    {
        parent::setUp();
    }

    /**
     * Get package providers.
     *
     * @param  \Illuminate\Foundation\Application  $app
     * @return array<int, class-string>
     */
    protected function getPackageProviders($app): array
    {
        return [
            ClickHouseServiceProvider::class,
        ];
    }

    /**
     * Define environment setup.
     *
     * @param  \Illuminate\Foundation\Application  $app
     * @return void
     */
    protected function getEnvironmentSetUp($app): void
    {
        $config = $app->get('config');

        $config->set('database.connections.clickhouse', [
            'driver' => 'clickhouse',
            'host' => env('CLICKHOUSE_HOST', '127.0.0.1'),
            'port' => env('CLICKHOUSE_PORT', 8123),
            'username' => env('CLICKHOUSE_USERNAME', 'default'),
            'password' => env('CLICKHOUSE_PASSWORD', ''),
            'database' => env('CLICKHOUSE_DATABASE', 'test'),
            'settings' => [
                'readonly' => 0,
                'max_execution_time' => 60,
            ],
        ]);
    }
}
