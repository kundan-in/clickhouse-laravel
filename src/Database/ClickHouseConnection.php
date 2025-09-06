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
        try {
            // ClickHouse doesn't support parameter placeholders in FORMAT JSON queries
            // We need to substitute the bindings manually
            $processedQuery = $this->substituteBindings($query, $bindings);

            $result = $this->client->select($processedQuery);

            // Handle both real Statement objects and mocked arrays for testing
            if (is_array($result)) {
                return $this->normalizeResults($result);
            }

            $rows = $result->rows();

            return $this->normalizeResults($rows);
        } catch (\Exception $e) {
            throw new \KundanIn\ClickHouseLaravel\Exceptions\ClickHouseException(
                "Failed to execute select query: {$e->getMessage()}",
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * Normalize ClickHouse results to be compatible with Laravel Eloquent.
     *
     * @param  array  $results
     * @return array
     */
    protected function normalizeResults(array $results): array
    {
        if (empty($results)) {
            return [];
        }

        // Check if we need normalization by scanning the first row
        $needsNormalization = false;
        if (! empty($results) && is_array($results[0])) {
            foreach ($results[0] as $value) {
                if (is_array($value) || is_object($value)) {
                    $needsNormalization = true;
                    break;
                }
            }
        }

        // Skip normalization if not needed for better performance
        if (! $needsNormalization) {
            return $results;
        }

        return array_map(function ($row) {
            if (! is_array($row)) {
                return $row;
            }

            // Convert complex data types to JSON strings for Laravel compatibility
            foreach ($row as $key => $value) {
                if (is_array($value)) {
                    // Convert all arrays to JSON for proper casting
                    $row[$key] = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
                } elseif (is_object($value)) {
                    // Convert objects to JSON
                    $row[$key] = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
                }
                // Leave scalar values (string, int, float, bool, null) as-is
            }

            return $row;
        }, $results);
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
        return $this->statement($query, $bindings);
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
        try {
            return $this->client->write($query, $bindings);
        } catch (\Exception $e) {
            throw new \KundanIn\ClickHouseLaravel\Exceptions\ClickHouseException(
                "Failed to execute statement: {$e->getMessage()}",
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * Run a select statement and return a single result.
     *
     * @param  string  $query  The SQL query
     * @param  array  $bindings  Query bindings
     * @param  bool  $useReadPdo  Whether to use read PDO (not applicable for ClickHouse)
     * @return mixed
     */
    public function selectOne($query, $bindings = [], $useReadPdo = true): mixed
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
    public function scalar($query, $bindings = [], $useReadPdo = true): mixed
    {
        $record = $this->selectOne($query, $bindings, $useReadPdo);
        if (! $record) {
            return null;
        }

        return is_array($record) ? reset($record) : $record;
    }

    /**
     * Substitute parameter bindings in the query.
     * ClickHouse doesn't support parameter placeholders in FORMAT JSON queries,
     * so we need to manually substitute the values.
     *
     * @param  string  $query  The SQL query with placeholders
     * @param  array  $bindings  The values to substitute
     * @return string The query with substituted values
     */
    protected function substituteBindings($query, $bindings): string
    {
        if (empty($bindings)) {
            return $query;
        }

        $processed = $query;
        $bindingIndex = 0;

        // Replace each ? with the corresponding binding value
        $processed = preg_replace_callback('/\?/', function ($matches) use ($bindings, &$bindingIndex) {
            if ($bindingIndex >= count($bindings)) {
                return '?'; // Keep the placeholder if no binding available
            }

            $value = $bindings[$bindingIndex++];

            return $this->quoteValue($value);
        }, $processed);

        return $processed;
    }

    /**
     * Quote a value for safe inclusion in SQL queries.
     * Implements comprehensive ClickHouse escaping to prevent SQL injection.
     *
     * @param  mixed  $value
     * @return string
     */
    protected function quoteValue($value): string
    {
        if (is_null($value)) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if (is_numeric($value) && ! is_string($value)) {
            // Ensure it's actually numeric and not a string that looks numeric
            return (string) $value;
        }

        // Comprehensive string escaping for ClickHouse
        $value = (string) $value;
        $escaped = str_replace([
            '\\',    // Backslash must be escaped first
            "'",     // Single quotes
            '"',     // Double quotes
            "\n",    // Newlines
            "\r",    // Carriage returns
            "\t",    // Tabs
            "\0",    // Null bytes
            "\x1a",  // EOF character
        ], [
            '\\\\',
            "\\'",
            '\\"',
            '\\n',
            '\\r',
            '\\t',
            '\\0',
            '\\Z',
        ], $value);

        return "'".$escaped."'";
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
     * @param  array  $bindings  Query bindings (ignored for ClickHouse ALTER UPDATE)
     * @return int
     */
    public function update($query, $bindings = []): int
    {
        // For ClickHouse ALTER UPDATE statements, bindings are already embedded in the query
        $this->client->write($query);

        return 1; // ClickHouse doesn't return affected row count easily
    }

    /**
     * Run a delete statement against the database.
     *
     * @param  string  $query  The SQL query
     * @param  array  $bindings  Query bindings (ignored for ClickHouse ALTER DELETE)
     * @return int
     */
    public function delete($query, $bindings = []): int
    {
        // For ClickHouse ALTER DELETE statements, bindings are already embedded in the query
        $this->client->write($query);

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

    /**
     * Get a new Eloquent query builder for the connection.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder
     */
    public function newEloquentBuilder($query)
    {
        return new ClickHouseEloquentBuilder($query);
    }

    /**
     * Get a new query builder instance.
     *
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseQueryBuilder
     */
    public function query()
    {
        return new ClickHouseQueryBuilder(
            $this, $this->getQueryGrammar(), $this->getPostProcessor()
        );
    }

    /**
     * Get a schema builder instance for the connection.
     *
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseSchemaBuilder
     */
    public function getSchemaBuilder()
    {
        if (is_null($this->schemaGrammar)) {
            $this->useDefaultSchemaGrammar();
        }

        return new ClickHouseSchemaBuilder($this);
    }

    /**
     * Perform a health check on the ClickHouse connection.
     *
     * @return bool
     *
     * @throws \KundanIn\ClickHouseLaravel\Exceptions\ClickHouseException
     */
    public function healthCheck(): bool
    {
        try {
            $result = $this->select('SELECT 1 as health_check');

            return ! empty($result) && isset($result[0]['health_check']) && $result[0]['health_check'] === 1;
        } catch (\Exception $e) {
            throw new \KundanIn\ClickHouseLaravel\Exceptions\ClickHouseException(
                "ClickHouse connection health check failed: {$e->getMessage()}",
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * Get ClickHouse server version information.
     *
     * @return string
     *
     * @throws \KundanIn\ClickHouseLaravel\Exceptions\ClickHouseException
     */
    public function getServerVersion(): string
    {
        try {
            $result = $this->select('SELECT version() as version');

            return $result[0]['version'] ?? 'Unknown';
        } catch (\Exception $e) {
            throw new \KundanIn\ClickHouseLaravel\Exceptions\ClickHouseException(
                "Failed to get ClickHouse server version: {$e->getMessage()}",
                $e->getCode(),
                $e
            );
        }
    }
}
