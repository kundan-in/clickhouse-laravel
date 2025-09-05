<?php

/**
 * ClickHouse Database Configuration
 *
 * This configuration file defines the default settings for ClickHouse connections.
 * You can override these values by setting the corresponding environment variables
 * in your .env file.
 */

return [
    /*
    |--------------------------------------------------------------------------
    | ClickHouse Host
    |--------------------------------------------------------------------------
    |
    | The hostname or IP address of your ClickHouse server.
    |
    */
    'host' => env('CLICKHOUSE_HOST', '127.0.0.1'),

    /*
    |--------------------------------------------------------------------------
    | ClickHouse Port
    |--------------------------------------------------------------------------
    |
    | The port number for HTTP connections to ClickHouse (default: 8123).
    | For HTTPS connections, typically use port 8443.
    |
    */
    'port' => env('CLICKHOUSE_PORT', 8123),

    /*
    |--------------------------------------------------------------------------
    | ClickHouse Username
    |--------------------------------------------------------------------------
    |
    | The username for authenticating with ClickHouse.
    |
    */
    'username' => env('CLICKHOUSE_USERNAME', 'default'),

    /*
    |--------------------------------------------------------------------------
    | ClickHouse Password
    |--------------------------------------------------------------------------
    |
    | The password for authenticating with ClickHouse.
    |
    */
    'password' => env('CLICKHOUSE_PASSWORD', ''),

    /*
    |--------------------------------------------------------------------------
    | ClickHouse Database
    |--------------------------------------------------------------------------
    |
    | The default database to connect to on the ClickHouse server.
    |
    */
    'database' => env('CLICKHOUSE_DATABASE', 'default'),

    /*
    |--------------------------------------------------------------------------
    | Connection Settings
    |--------------------------------------------------------------------------
    |
    | Additional connection settings for ClickHouse.
    |
    */
    'settings' => [
        'readonly' => env('CLICKHOUSE_READONLY', 0),
        'max_execution_time' => env('CLICKHOUSE_MAX_EXECUTION_TIME', 60),
    ],
];
