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
 * ClickHouse database connection for Laravel.
 *
 * Extends Laravel's base Connection class to provide ClickHouse connectivity
 * using the smi2/phpClickHouse Client library. Handles query execution,
 * parameter binding, result normalization, and automatic database prefixing
 * for all SQL operations.
 */
class ClickHouseConnection extends Connection
{
    /**
     * The ClickHouse client instance.
     *
     * @var \ClickHouseDB\Client
     */
    protected Client $client;

    /**
     * Create a new ClickHouse connection instance.
     *
     * Initializes the underlying ClickHouse client with the provided
     * configuration and applies timeout settings via the client API.
     *
     * @param  array{host?: string, port?: int, username?: string, password?: string, database?: string, timeout?: int, connect_timeout?: float, settings?: array, prefix?: string}  $config
     *
     * @throws \InvalidArgumentException
     */
    public function __construct(array $config)
    {
        $this->config = $config;

        $clickhouseConfig = [
            'host' => $config['host'] ?? '127.0.0.1',
            'port' => $config['port'] ?? 8123,
            'username' => $config['username'] ?? 'default',
            'password' => $config['password'] ?? '',
            'database' => $config['database'] ?? 'default',
        ];

        if (isset($config['settings'])) {
            $clickhouseConfig['settings'] = $config['settings'];
        }

        $this->client = new Client($clickhouseConfig);

        $this->applyTimeoutSettings($config);

        parent::__construct(
            null,
            $config['database'] ?? '',
            $config['prefix'] ?? '',
            $config
        );
    }

    /**
     * Apply timeout settings to the ClickHouse client.
     *
     * Configures both the TCP connection timeout and the query execution /
     * HTTP request timeout using the smi2/phpClickHouse client API.
     *
     * @param  array{timeout?: int, connect_timeout?: float}  $config
     */
    protected function applyTimeoutSettings(array $config): void
    {
        if (isset($config['connect_timeout'])) {
            $this->client->setConnectTimeOut((float) $config['connect_timeout']);
        }

        if (isset($config['timeout'])) {
            $this->client->setTimeout((int) $config['timeout']);
        }
    }

    /**
     * Run a select statement and return the result.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @param  bool  $useReadPdo
     * @param  array  $fetchUsing
     * @return array
     *
     * @throws \KundanIn\ClickHouseLaravel\Exceptions\ClickHouseException
     */
    public function select($query, $bindings = [], $useReadPdo = true, array $fetchUsing = []): array
    {
        try {
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
     * Normalize ClickHouse results for Laravel Eloquent compatibility.
     *
     * Converts complex data types (arrays, objects) to JSON strings so
     * that Laravel's attribute casting can handle them properly.
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
     * @param  string  $query
     * @param  array  $bindings
     * @return bool
     */
    public function insert($query, $bindings = []): bool
    {
        return $this->statement($query, $bindings);
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return bool
     *
     * @throws \KundanIn\ClickHouseLaravel\Exceptions\ClickHouseException
     */
    public function statement($query, $bindings = []): bool
    {
        try {
            $processedQuery = $this->processTableReferences($query);

            $result = $this->client->write($processedQuery, $bindings);

            // Handle different return types from ClickHouse client
            if (is_bool($result)) {
                return $result;
            }

            // If it's a Statement object or other truthy value, consider it successful
            return $result !== null && $result !== false;
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
     * @param  string  $query
     * @param  array  $bindings
     * @param  bool  $useReadPdo
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
     * @param  string  $query
     * @param  array  $bindings
     * @param  bool  $useReadPdo
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
     * Substitute parameter bindings into the query string.
     *
     * ClickHouse does not support parameter placeholders in FORMAT JSON
     * queries, so values are manually substituted and escaped.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return string
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
     * Quote a value for safe inclusion in ClickHouse SQL queries.
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
     * Prepend the configured database name to unqualified table references.
     *
     * @param  string  $query
     * @return string
     */
    protected function processTableReferences(string $query): string
    {
        $database = $this->getDatabaseName();

        // If no database is configured, return as-is
        if (empty($database) || $database === 'default') {
            return $query;
        }

        // Check if the query already has database prefixes by looking for patterns like:
        // FROM database.table, JOIN database.table, TABLE database.table, etc.
        if (preg_match('/(?:FROM|JOIN|TABLE|INTO)\s+[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\b/i', $query)) {
            return $query;
        }

        // Pattern to match table references in common SQL statements
        // This matches table names that follow keywords like TABLE, FROM, JOIN, INTO, etc.
        $patterns = [
            // OPTIMIZE TABLE table_name
            '/\bOPTIMIZE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/i',
            // DROP TABLE table_name
            '/\bDROP\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/i',
            // CREATE TABLE table_name
            '/\bCREATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/i',
            // TRUNCATE TABLE table_name
            '/\bTRUNCATE\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/i',
            // INSERT INTO table_name
            '/\bINSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/i',
            // FROM table_name
            '/\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/i',
            // JOIN table_name (covers INNER JOIN, LEFT JOIN, RIGHT JOIN, etc.)
            '/\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/i',
            // ALTER TABLE table_name (for ClickHouse UPDATE/DELETE operations)
            '/\bALTER\s+TABLE\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/i',
        ];

        foreach ($patterns as $pattern) {
            $query = preg_replace_callback($pattern, function ($matches) use ($database) {
                return str_replace($matches[1], $database.'.'.$matches[1], $matches[0]);
            }, $query);
        }

        return $query;
    }

    /**
     * Get a query builder instance for the given table.
     *
     * @param  string  $table
     * @param  string|null  $as
     * @return \Illuminate\Database\Query\Builder
     */
    public function table($table, $as = null): QueryBuilder
    {
        return $this->query()->from($table, $as);
    }

    /**
     * Get a new raw query expression.
     *
     * @param  mixed  $value
     * @return \Illuminate\Database\Query\Expression
     */
    public function raw($value): Expression
    {
        return new Expression($value);
    }

    /**
     * Run an update statement against the database.
     *
     * Bindings are already embedded in ALTER UPDATE queries by the grammar.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return int
     */
    public function update($query, $bindings = []): int
    {
        $this->client->write($query);

        return 1;
    }

    /**
     * Run a delete statement against the database.
     *
     * Bindings are already embedded in ALTER DELETE queries by the grammar.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return int
     */
    public function delete($query, $bindings = []): int
    {
        $this->client->write($query);

        return 1;
    }

    /**
     * Run an SQL statement and return the number of affected rows.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return int
     */
    public function affectingStatement($query, $bindings = []): int
    {
        $this->client->write($query, $bindings);

        return 1;
    }

    /**
     * Run a raw, unprepared query against the database.
     *
     * @param  string  $query
     * @return bool
     */
    public function unprepared($query): bool
    {
        return $this->client->write($query);
    }

    /**
     * Insert a batch of rows using ClickHouse's native bulk insert.
     *
     * This is significantly faster than row-by-row INSERT for large datasets.
     * Uses the smi2/phpClickHouse client's native insert which sends data
     * in ClickHouse's columnar format.
     *
     * @param  string  $table
     * @param  array  $rows  Array of associative arrays.
     * @param  array  $columns  Column names (auto-detected from first row if empty).
     * @return bool
     *
     * @throws \KundanIn\ClickHouseLaravel\Exceptions\ClickHouseException
     */
    public function bulkInsert(string $table, array $rows, array $columns = []): bool
    {
        if (empty($rows)) {
            return true;
        }

        if (empty($columns)) {
            $columns = array_keys($rows[0]);
        }

        try {
            $this->client->insert($table, $rows, $columns);

            return true;
        } catch (\Exception $e) {
            throw new \KundanIn\ClickHouseLaravel\Exceptions\ClickHouseException(
                "Bulk insert failed: {$e->getMessage()}",
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * Run a select statement and return a generator for streaming results.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @param  bool  $useReadPdo
     * @return \Generator
     */
    public function cursor($query, $bindings = [], $useReadPdo = true, array $fetchUsing = [])
    {
        $results = $this->select($query, $bindings, $useReadPdo);

        foreach ($results as $row) {
            yield (object) $row;
        }
    }

    /**
     * Execute a closure within a "transaction".
     *
     * ClickHouse has limited transaction support, so the callback
     * is executed directly without transactional guarantees.
     *
     * @param  \Closure  $callback
     * @param  int  $attempts
     * @return mixed
     */
    public function transaction(Closure $callback, $attempts = 1)
    {
        return $callback();
    }

    /**
     * Start a new database transaction.
     *
     * No-op: ClickHouse does not support traditional transactions.
     *
     * @return void
     */
    public function beginTransaction(): void
    {
        //
    }

    /**
     * Commit the active database transaction.
     *
     * No-op: ClickHouse does not support traditional transactions.
     *
     * @return void
     */
    public function commit(): void
    {
        //
    }

    /**
     * Rollback the active database transaction.
     *
     * No-op: ClickHouse does not support traditional transactions.
     *
     * @param  int|null  $toLevel
     * @return void
     */
    public function rollBack($toLevel = null): void
    {
        //
    }

    /**
     * Get the number of active transactions.
     *
     * @return int
     */
    public function transactionLevel(): int
    {
        return 0;
    }

    /**
     * Execute queries in "dry run" mode.
     *
     * @param  \Closure  $callback
     * @return array
     */
    public function pretend(Closure $callback): array
    {
        return [];
    }

    /**
     * Get the name of the database driver.
     *
     * @return string
     */
    public function getDriverName(): string
    {
        return 'clickhouse';
    }

    /**
     * Disconnect from the database.
     *
     * @return void
     */
    public function disconnect(): void
    {
        // ClickHouse uses stateless HTTP — no persistent connection to close.
    }

    /**
     * Get the name of the connected database.
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
     * @return \Illuminate\Database\Grammar
     */
    protected function getDefaultQueryGrammar(): Grammar
    {
        return $this->applyGrammarDefaults(new ClickHouseQueryGrammar($this));
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return \Illuminate\Database\Grammar
     */
    protected function getDefaultSchemaGrammar(): Grammar
    {
        return $this->applyGrammarDefaults(new ClickHouseSchemaGrammar($this));
    }

    /**
     * Apply connection and table prefix defaults to a grammar instance.
     *
     * Handles compatibility across Laravel versions where the Grammar
     * initialization API changed between v11 and v12.
     *
     * @param  \Illuminate\Database\Grammar  $grammar
     * @return \Illuminate\Database\Grammar
     */
    protected function applyGrammarDefaults(Grammar $grammar): Grammar
    {
        // Laravel 11: setConnection() exists, withTablePrefix() exists
        // Laravel 12+: connection set via constructor, no setConnection/withTablePrefix
        if (method_exists($grammar, 'setConnection')) {
            $grammar->setConnection($this);
        }

        if (method_exists($this, 'withTablePrefix')) {
            return $this->withTablePrefix($grammar);
        }

        return $grammar;
    }

    /**
     * Get the default post processor instance.
     *
     * @return \Illuminate\Database\Query\Processors\Processor
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
     * Determine if the ClickHouse server is reachable.
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
     * Get the ClickHouse server version string.
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
