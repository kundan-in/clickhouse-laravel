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
 *
 * @package KundanIn\ClickHouseLaravel
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
            __DIR__ . '/../config/clickhouse.php' => config_path('clickhouse.php'),
        ], 'clickhouse-config');
    }

    /**
     * Register any application services.
     *
     * @return void
     */
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/clickhouse.php', 'clickhouse');

        $this->app->resolving('db', function (DatabaseManager $db): void {
            $db->extend('clickhouse', function (array $config, string $name): ClickHouseConnection {
                return new ClickHouseConnection($config);
            });
        });
    }
}
