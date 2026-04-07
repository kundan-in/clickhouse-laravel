<?php

namespace KundanIn\ClickHouseLaravel\Database;

use ClickHouseDB\Client;
use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use InvalidArgumentException;

/**
 * ClickHouse database connector for Laravel.
 *
 * Extends Laravel's base Connector to establish connections to
 * ClickHouse using the smi2/phpClickHouse Client library.
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
     * Create a new ClickHouse client instance.
     *
     * @param  string  $dsn
     * @param  array  $config
     * @param  array  $options
     * @return \ClickHouseDB\Client
     *
     * @throws \InvalidArgumentException
     */
    public function createConnection($dsn, array $config, array $options)
    {
        $clientConfig = [
            'host' => $config['host'] ?? '127.0.0.1',
            'port' => $config['port'] ?? 8123,
            'username' => $config['username'] ?? 'default',
            'password' => $config['password'] ?? '',
            'database' => $config['database'] ?? 'default',
        ];

        if (isset($config['settings']) && is_array($config['settings'])) {
            $clientConfig['settings'] = $config['settings'];
        }

        if (isset($config['https']) && $config['https']) {
            $clientConfig['https'] = true;
        }

        try {
            $client = new Client($clientConfig);

            if (isset($config['connect_timeout'])) {
                $client->setConnectTimeOut((float) $config['connect_timeout']);
            }

            if (isset($config['timeout'])) {
                $client->setTimeout((int) $config['timeout']);
            }

            return $client;
        } catch (\Exception $e) {
            throw new InvalidArgumentException("Could not create ClickHouse connection: {$e->getMessage()}", 0, $e);
        }
    }

    /**
     * Build a DSN string from the given configuration.
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
     * Get the connection options from the configuration.
     *
     * @param  array  $config
     * @return array
     */
    public function getOptions(array $config): array
    {
        return $config['options'] ?? [];
    }

    /**
     * Get the default connection options.
     *
     * @return array
     */
    public function getDefaultOptions(): array
    {
        return [];
    }

    /**
     * Configure the connection character set.
     *
     * ClickHouse uses UTF-8 by default; this is a no-op for compatibility.
     *
     * @param  \ClickHouseDB\Client  $connection
     * @param  array  $config
     */
    protected function configureCharset($connection, array $config): void
    {
        //
    }

    /**
     * Configure the timezone on the connection.
     *
     * @param  \ClickHouseDB\Client  $connection
     * @param  array  $config
     */
    protected function configureTimezone($connection, array $config): void
    {
        if (isset($config['timezone'])) {
            // ClickHouse timezone configuration can be applied via session settings.
        }
    }

    /**
     * Create a Laravel-compatible ClickHouse connection instance.
     *
     * @param  array  $config
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseConnection
     */
    public function createLaravelConnection(array $config): ClickHouseConnection
    {
        return new ClickHouseConnection($config);
    }
}
