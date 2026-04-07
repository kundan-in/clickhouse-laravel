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
    | Request Timeout
    |--------------------------------------------------------------------------
    |
    | The maximum number of seconds to wait for a query to complete. This
    | controls both the ClickHouse server-side max_execution_time setting
    | and the HTTP client timeout (CURLOPT_TIMEOUT). Set to 0 for no limit.
    |
    */
    'timeout' => env('CLICKHOUSE_TIMEOUT', 120),

    /*
    |--------------------------------------------------------------------------
    | Connection Timeout
    |--------------------------------------------------------------------------
    |
    | The maximum number of seconds to wait while trying to establish a
    | connection to the ClickHouse server (CURLOPT_CONNECTTIMEOUT).
    |
    */
    'connect_timeout' => env('CLICKHOUSE_CONNECT_TIMEOUT', 5),

    /*
    |--------------------------------------------------------------------------
    | Connection Settings
    |--------------------------------------------------------------------------
    |
    | Additional ClickHouse server-side settings applied to each query.
    | See https://clickhouse.com/docs/en/operations/settings for all
    | available settings.
    |
    */
    'settings' => [
        'readonly' => env('CLICKHOUSE_READONLY', 0),
        'max_execution_time' => env('CLICKHOUSE_MAX_EXECUTION_TIME', 60),
    ],
];
