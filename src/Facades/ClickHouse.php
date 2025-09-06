<?php

namespace KundanIn\ClickHouseLaravel\Facades;

use Illuminate\Support\Facades\Facade;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;

/**
 * ClickHouse Facade
 *
 * Provides convenient access to ClickHouse database operations.
 *
 * @method static array select(string $query, array $bindings = [])
 * @method static mixed selectOne(string $query, array $bindings = [])
 * @method static mixed scalar(string $query, array $bindings = [])
 * @method static bool insert(string $query, array $bindings = [])
 * @method static bool statement(string $query, array $bindings = [])
 * @method static int update(string $query, array $bindings = [])
 * @method static int delete(string $query, array $bindings = [])
 * @method static bool healthCheck()
 * @method static string getServerVersion()
 * @method static \KundanIn\ClickHouseLaravel\Database\ClickHouseQueryBuilder table(string $table)
 * @method static \KundanIn\ClickHouseLaravel\Database\ClickHouseQueryBuilder query()
 * @method static \Illuminate\Database\Query\Expression raw(mixed $value)
 *
 * @see \KundanIn\ClickHouseLaravel\Database\ClickHouseConnection
 */
class ClickHouse extends Facade
{
    /**
     * Get the registered name of the component.
     *
     * @return string
     */
    protected static function getFacadeAccessor(): string
    {
        return 'clickhouse';
    }

    /**
     * Get the root object behind the facade.
     *
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseConnection
     */
    public static function connection(?string $name = null): ClickHouseConnection
    {
        return app('db')->connection($name ?: 'clickhouse');
    }
}
