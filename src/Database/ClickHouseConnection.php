<?php

namespace KundanIn\ClickHouseLaravel\Database;

use ClickHouseDB\Client;
use Closure;
use Illuminate\Database\Connection;
use Illuminate\Database\Grammar;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Database\Query\Expression;
use Illuminate\Database\Query\Processors\Processor;

/**
 * ClickHouse Database Connection
 *
 * This class extends Laravel's Connection class to provide ClickHouse database
 * connectivity using the smi2/phpClickHouse client library.
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
     * @param  array  $config  The database connection configuration
     */
    public function __construct(array $config)
    {
        $this->config = $config;

        // Transform Laravel config to ClickHouse client config format
        $clickhouseConfig = [
            'host' => $config['host'] ?? '127.0.0.1',
            'port' => $config['port'] ?? 8123,
            'username' => $config['username'] ?? 'default',
            'password' => $config['password'] ?? '',
            'database' => $config['database'] ?? 'default',
        ];

        // Add any additional settings
        if (isset($config['settings'])) {
            $clickhouseConfig['settings'] = $config['settings'];
        }

        $this->client = new Client($clickhouseConfig);

        // Initialize the connection with proper grammar and post processor
        parent::__construct(
            null, // PDO is not used for ClickHouse
            $config['database'] ?? '',
            $config['prefix'] ?? '',
            $config
        );
    }

    /**
     * Run a select statement and return the result.
     *
     * @param  string  $query  The SQL query
     * @param  array  $bindings  Query bindings
     * @param  bool  $useReadPdo  Whether to use read PDO (not applicable for ClickHouse)
     * @return array
     */
    public function select($query, $bindings = [], $useReadPdo = true): array
    {
        $result = $this->client->select($query, $bindings);

        // Handle both real Statement objects and mocked arrays for testing
        if (is_array($result)) {
            return $result;
        }

        return $result->rows();
    }

    /**
     * Run an insert statement against the database.
     *
     * @param  string  $query  The SQL query
     * @param  array  $bindings  Query bindings
     * @return bool
     */
    public function insert($query, $bindings = []): bool
    {
        return $this->client->write($query, $bindings);
    }

    /**
     * Execute an SQL statement and return the result.
     *
     * @param  string  $query  The SQL query
     * @param  array  $bindings  Query bindings
     * @return bool
     */
    public function statement($query, $bindings = []): bool
    {
        return $this->client->write($query, $bindings);
    }

    /**
     * Run a select statement and return a single result.
     *
     * @param  string  $query  The SQL query
     * @param  array  $bindings  Query bindings
     * @param  bool  $useReadPdo  Whether to use read PDO (not applicable for ClickHouse)
     * @return mixed
     */
    public function selectOne($query, $bindings = [], $useReadPdo = true)
    {
        $result = $this->select($query, $bindings, $useReadPdo);

        return reset($result) ?: null;
    }

    /**
     * Run a select statement and return a scalar result.
     *
     * @param  string  $query  The SQL query
     * @param  array  $bindings  Query bindings
     * @param  bool  $useReadPdo  Whether to use read PDO (not applicable for ClickHouse)
     * @return mixed
     */
    public function scalar($query, $bindings = [], $useReadPdo = true)
    {
        $record = $this->selectOne($query, $bindings, $useReadPdo);
        if (! $record) {
            return null;
        }

        return is_array($record) ? reset($record) : $record;
    }

    /**
     * Get a query builder for the table.
     *
     * @param  string  $table
     * @param  string|null  $as
     * @return QueryBuilder
     */
    public function table($table, $as = null): QueryBuilder
    {
        return $this->query()->from($table, $as);
    }

    /**
     * Get a new raw query expression.
     *
     * @param  mixed  $value
     * @return Expression
     */
    public function raw($value): Expression
    {
        return new Expression($value);
    }

    /**
     * Run an update statement against the database.
     *
     * @param  string  $query  The SQL query
     * @param  array  $bindings  Query bindings
     * @return int
     */
    public function update($query, $bindings = []): int
    {
        $this->client->write($query, $bindings);

        return 1; // ClickHouse doesn't return affected row count easily
    }

    /**
     * Run a delete statement against the database.
     *
     * @param  string  $query  The SQL query
     * @param  array  $bindings  Query bindings
     * @return int
     */
    public function delete($query, $bindings = []): int
    {
        $this->client->write($query, $bindings);

        return 1; // ClickHouse doesn't return affected row count easily
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param  string  $query  The SQL query
     * @param  array  $bindings  Query bindings
     * @return int
     */
    public function affectingStatement($query, $bindings = []): int
    {
        $this->client->write($query, $bindings);

        return 1; // ClickHouse doesn't return affected row count easily
    }

    /**
     * Run a raw, unprepared query against the PDO connection.
     *
     * @param  string  $query  The SQL query
     * @return bool
     */
    public function unprepared($query): bool
    {
        return $this->client->write($query);
    }

    /**
     * Execute a Closure within a transaction.
     *
     * @param  Closure  $callback
     * @param  int  $attempts
     * @return mixed
     *
     * @throws \Throwable
     */
    public function transaction(Closure $callback, $attempts = 1)
    {
        // ClickHouse has limited transaction support, so we just execute the callback
        return $callback();
    }

    /**
     * Start a new database transaction.
     *
     * @return void
     */
    public function beginTransaction(): void
    {
        // ClickHouse doesn't support traditional transactions
        // This is a no-op for compatibility
    }

    /**
     * Commit the active database transaction.
     *
     * @return void
     */
    public function commit(): void
    {
        // ClickHouse doesn't support traditional transactions
        // This is a no-op for compatibility
    }

    /**
     * Rollback the active database transaction.
     *
     * @param  int|null  $toLevel
     * @return void
     */
    public function rollBack($toLevel = null): void
    {
        // ClickHouse doesn't support traditional transactions
        // This is a no-op for compatibility
    }

    /**
     * Get the number of active transactions.
     *
     * @return int
     */
    public function transactionLevel(): int
    {
        // ClickHouse doesn't support nested transactions
        return 0;
    }

    /**
     * Execute queries in "dry run" mode by logging them.
     *
     * @param  Closure  $callback
     * @return array
     */
    public function pretend(Closure $callback): array
    {
        return [];
    }

    /**
     * Get the database name.
     *
     * @return string
     */
    public function getDatabaseName(): string
    {
        return $this->getConfig('database') ?? '';
    }

    /**
     * Get the default query grammar instance.
     *
     * @return Grammar
     */
    protected function getDefaultQueryGrammar(): Grammar
    {
        return new ClickHouseQueryGrammar($this);
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return Grammar
     */
    protected function getDefaultSchemaGrammar(): Grammar
    {
        return new ClickHouseSchemaGrammar($this);
    }

    /**
     * Get the default post processor instance.
     *
     * @return Processor
     */
    protected function getDefaultPostProcessor(): Processor
    {
        return new Processor;
    }
}
