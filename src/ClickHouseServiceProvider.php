<?php

namespace KundanIn\ClickHouseLaravel;

use Illuminate\Database\DatabaseManager;
use Illuminate\Support\ServiceProvider;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;

/**
 * ClickHouse service provider for Laravel.
 *
 * Registers the ClickHouse database driver with Laravel's DatabaseManager
 * and publishes the package configuration file.
 */
class ClickHouseServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap any application services.
     */
    public function boot(): void
    {
        $this->publishes([
            __DIR__.'/../config/clickhouse.php' => config_path('clickhouse.php'),
        ], 'clickhouse-config');
    }

    /**
     * Register any application services.
     */
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/clickhouse.php', 'clickhouse');

        $this->app->resolving('db', function (DatabaseManager $db): void {
            $db->extend('clickhouse', function (array $config, string $name): ClickHouseConnection {
                $config = array_merge([
                    'driver' => 'clickhouse',
                    'host' => config('clickhouse.host', '127.0.0.1'),
                    'port' => config('clickhouse.port', 8123),
                    'database' => config('clickhouse.database', 'default'),
                    'username' => config('clickhouse.username', 'default'),
                    'password' => config('clickhouse.password', ''),
                    'prefix' => '',
                    'timeout' => config('clickhouse.timeout', 120),
                    'connect_timeout' => config('clickhouse.connect_timeout', 5),
                    'settings' => config('clickhouse.settings', []),
                ], $config);

                return new ClickHouseConnection($config);
            });
        });

        $this->app->bind('clickhouse', function ($app) {
            return $app['db']->connection('clickhouse');
        });
    }

    /**
     * Get the published paths for the service provider.
     *
     * @return array<string, array<string, string>>
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
