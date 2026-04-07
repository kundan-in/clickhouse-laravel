<?php

namespace KundanIn\ClickHouseLaravel\Tests;

use KundanIn\ClickHouseLaravel\ClickHouseServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

/**
 * Base test case for the ClickHouse Laravel package.
 *
 * Sets up the Laravel testing environment with the ClickHouse
 * service provider and a default database connection.
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
     * Get the package providers to register.
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
     * Define the environment setup for testing.
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
            'timeout' => 120,
            'connect_timeout' => 5,
            'settings' => [
                'readonly' => 0,
                'max_execution_time' => 60,
            ],
        ]);
    }
}
