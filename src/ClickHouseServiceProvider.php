<?php

namespace KundanIn\ClickHouseLaravel;

use Illuminate\Database\DatabaseManager;
use Illuminate\Support\ServiceProvider;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;

/**
 * ClickHouse Service Provider for Laravel
 *
 * This service provider registers the ClickHouse database driver with Laravel's
 * database manager and publishes the configuration file.
 */
class ClickHouseServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap any application services.
     *
     * @return void
     */
    public function boot(): void
    {
        $this->publishes([
            __DIR__.'/../config/clickhouse.php' => config_path('clickhouse.php'),
        ], 'clickhouse-config');
    }

    /**
     * Register any application services.
     *
     * @return void
     */
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/clickhouse.php', 'clickhouse');

        $this->app->resolving('db', function (DatabaseManager $db): void {
            $db->extend('clickhouse', function (array $config, string $name): ClickHouseConnection {
                // Ensure we have all required connection parameters
                $config = array_merge([
                    'driver' => 'clickhouse',
                    'host' => config('clickhouse.host', '127.0.0.1'),
                    'port' => config('clickhouse.port', 8123),
                    'database' => config('clickhouse.database', 'default'),
                    'username' => config('clickhouse.username', 'default'),
                    'password' => config('clickhouse.password', ''),
                    'prefix' => '',
                    'settings' => config('clickhouse.settings', []),
                ], $config);

                return new ClickHouseConnection($config);
            });
        });
    }

    /**
     * Get the published paths for the service provider.
     *
     * @return array
     */
    public function publishedPaths(): array
    {
        return [
            'clickhouse-config' => [
                __DIR__.'/../config/clickhouse.php' => config_path('clickhouse.php'),
            ],
        ];
    }
}
