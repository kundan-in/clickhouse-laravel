# ClickHouse Laravel

A Laravel package that provides seamless integration with ClickHouse database, enabling you to use ClickHouse as a database connection in your Laravel applications.

## Features

- Easy integration with Laravel's database system
- Support for Laravel 10.x, 11.x, and 12.x
- Simple configuration through environment variables
- Laravel-style query building and model support
- Compatible with PHP 8.1, 8.2, and 8.3

## Installation

Install the package via Composer:

```bash
composer require kundan-in/clickhouse-laravel
```

## Configuration

### 1. Publish Configuration (Optional)

```bash
php artisan vendor:publish --provider="KundanIn\ClickHouseLaravel\ClickHouseServiceProvider" --tag="clickhouse-config"
```

### 2. Environment Variables

Add the following environment variables to your `.env` file:

```env
CLICKHOUSE_HOST=127.0.0.1
CLICKHOUSE_PORT=8123
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=default
```

### 3. Database Configuration

Add a ClickHouse connection to your `config/database.php`:

```php
'connections' => [
    // ... other connections
    
    'clickhouse' => [
        'driver' => 'clickhouse',
        'host' => env('CLICKHOUSE_HOST', '127.0.0.1'),
        'port' => env('CLICKHOUSE_PORT', 8123),
        'username' => env('CLICKHOUSE_USERNAME', 'default'),
        'password' => env('CLICKHOUSE_PASSWORD', ''),
        'database' => env('CLICKHOUSE_DATABASE', 'default'),
        'settings' => [
            'readonly' => env('CLICKHOUSE_READONLY', 0),
            'max_execution_time' => env('CLICKHOUSE_MAX_EXECUTION_TIME', 60),
        ],
    ],
],
```

## Usage

### Using with Models

To use ClickHouse with Eloquent models, specify the connection in your model:

```php
<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class AnalyticsEvent extends Model
{
    protected $connection = 'clickhouse';
    protected $table = 'events';
    
    protected $fillable = [
        'user_id',
        'event_type',
        'event_data',
        'timestamp',
    ];
}
```

### Query Builder

You can also use Laravel's query builder directly:

```php
use Illuminate\Support\Facades\DB;

// Insert data
DB::connection('clickhouse')->table('events')->insert([
    'user_id' => 1,
    'event_type' => 'page_view',
    'event_data' => json_encode(['page' => '/home']),
    'timestamp' => now(),
]);

// Select data
$events = DB::connection('clickhouse')
    ->table('events')
    ->where('user_id', 1)
    ->get();
```

### Raw Queries

For complex ClickHouse-specific queries:

```php
$results = DB::connection('clickhouse')->select('
    SELECT 
        event_type,
        count() as total
    FROM events 
    WHERE timestamp >= ? 
    GROUP BY event_type
', [now()->subDays(7)]);
```

## Requirements

- PHP 8.1 or higher
- Laravel 10.x, 11.x, or 12.x
- ClickHouse server

## Dependencies

This package uses the following dependencies:

- `smi2/phpClickHouse` - ClickHouse client for PHP
- `illuminate/database` - Laravel database components
- `illuminate/support` - Laravel support components

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This package is open-sourced software licensed under the [MIT license](LICENSE).

## Support

If you discover any security vulnerabilities or bugs, please create an issue in the [GitHub repository](https://github.com/kundan-in/clickhouse-laravel).

## Changelog

### v1.0.0
- Initial release
- Basic ClickHouse connection support
- Laravel 10.x, 11.x, 12.x compatibility
- Configuration publishing
- Query builder integration