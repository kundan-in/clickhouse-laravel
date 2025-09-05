<?php

namespace KundanIn\ClickHouseLaravel\Database;

use ClickHouseDB\Client;
use Illuminate\Database\Connection;

/**
 * ClickHouse Database Connection
 *
 * This class extends Laravel's Connection class to provide ClickHouse database
 * connectivity using the smi2/phpClickHouse client library.
 *
 * @package KundanIn\ClickHouseLaravel\Database
 */
class ClickHouseConnection extends Connection
{
    /**
     * The ClickHouse client instance.
     *
     * @var Client
     */
    protected Client $client;

    /**
     * Create a new ClickHouse connection instance.
     *
     * @param array $config The database connection configuration
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        $this->client = new Client($config);
    }

    /**
     * Run a select statement and return the result.
     *
     * @param string $query The SQL query
     * @param array $bindings Query bindings
     * @param bool $useReadPdo Whether to use read PDO (not applicable for ClickHouse)
     * @return array
     */
    public function select($query, $bindings = [], $useReadPdo = true): array
    {
        return $this->client->select($query, $bindings);
    }

    /**
     * Run an insert statement against the database.
     *
     * @param string $query The SQL query
     * @param array $bindings Query bindings
     * @return bool
     */
    public function insert($query, $bindings = []): bool
    {
        return $this->client->write($query, $bindings);
    }

    /**
     * Execute an SQL statement and return the result.
     *
     * @param string $query The SQL query
     * @param array $bindings Query bindings
     * @return bool
     */
    public function statement($query, $bindings = []): bool
    {
        return $this->client->write($query, $bindings);
    }
}
