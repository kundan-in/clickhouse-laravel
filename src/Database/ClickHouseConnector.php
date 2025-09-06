<?php

namespace KundanIn\ClickHouseLaravel\Database;

use ClickHouseDB\Client;
use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use InvalidArgumentException;

/**
 * ClickHouse Database Connector
 *
 * This class extends Laravel's Connector to provide ClickHouse database
 * connectivity using the smi2/phpClickHouse client library.
 */
class ClickHouseConnector extends Connector implements ConnectorInterface
{
    /**
     * Establish a database connection.
     *
     * @param  array  $config
     * @return \ClickHouseDB\Client
     *
     * @throws \InvalidArgumentException
     */
    public function connect(array $config)
    {
        $dsn = $this->getDsn($config);

        $options = $this->getOptions($config);

        return $this->createConnection($dsn, $config, $options);
    }

    /**
     * Create a new ClickHouse connection.
     *
     * @param  string  $dsn
     * @param  array  $config
     * @param  array  $options
     * @return \ClickHouseDB\Client
     */
    protected function createConnection($dsn, array $config, array $options)
    {
        $clientConfig = [
            'host' => $config['host'] ?? '127.0.0.1',
            'port' => $config['port'] ?? 8123,
            'username' => $config['username'] ?? 'default',
            'password' => $config['password'] ?? '',
            'database' => $config['database'] ?? 'default',
        ];

        // Add any additional settings
        if (isset($config['settings']) && is_array($config['settings'])) {
            $clientConfig['settings'] = $config['settings'];
        }

        // Handle timeout settings
        if (isset($config['timeout'])) {
            $clientConfig['timeout'] = $config['timeout'];
        }

        if (isset($config['connect_timeout'])) {
            $clientConfig['connect_timeout'] = $config['connect_timeout'];
        }

        // Handle SSL settings
        if (isset($config['https']) && $config['https']) {
            $clientConfig['https'] = true;
        }

        try {
            return new Client($clientConfig);
        } catch (\Exception $e) {
            throw new InvalidArgumentException("Could not create ClickHouse connection: {$e->getMessage()}", 0, $e);
        }
    }

    /**
     * Create a DSN string from a configuration.
     * Note: ClickHouse client doesn't use traditional PDO DSN format,
     * but we provide this for compatibility.
     *
     * @param  array  $config
     * @return string
     */
    protected function getDsn(array $config): string
    {
        $host = $config['host'] ?? '127.0.0.1';
        $port = $config['port'] ?? 8123;
        $database = $config['database'] ?? 'default';

        return "clickhouse:host={$host};port={$port};dbname={$database}";
    }

    /**
     * Get the PDO options based on the configuration.
     * Note: ClickHouse doesn't use PDO, but we maintain compatibility.
     *
     * @param  array  $config
     * @return array
     */
    protected function getOptions(array $config): array
    {
        return $config['options'] ?? [];
    }

    /**
     * Get the default PDO connection options.
     *
     * @return array
     */
    protected function getDefaultOptions(): array
    {
        return [];
    }

    /**
     * Set the connection character set and collation.
     * Note: ClickHouse handles charset differently, this is for compatibility.
     *
     * @param  \ClickHouseDB\Client  $connection
     * @param  array  $config
     * @return void
     */
    protected function configureCharset($connection, array $config): void
    {
        // ClickHouse typically uses UTF-8 by default
        // Additional charset configuration can be added here if needed
    }

    /**
     * Configure the timezone on the connection.
     *
     * @param  \ClickHouseDB\Client  $connection
     * @param  array  $config
     * @return void
     */
    protected function configureTimezone($connection, array $config): void
    {
        if (isset($config['timezone'])) {
            // ClickHouse timezone configuration can be added here
            // This might involve setting session timezone or connection settings
        }
    }

    /**
     * Create a Laravel database connection instance.
     *
     * @param  array  $config
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseConnection
     */
    public function createLaravelConnection(array $config): ClickHouseConnection
    {
        return new ClickHouseConnection($config);
    }
}
