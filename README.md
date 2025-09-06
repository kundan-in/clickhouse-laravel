# ClickHouse Laravel

[![Latest Version on Packagist](https://img.shields.io/packagist/v/kundan-in/clickhouse-laravel.svg?style=flat-square)](https://packagist.org/packages/kundan-in/clickhouse-laravel)
[![Tests](https://img.shields.io/github/actions/workflow/status/kundan-in/clickhouse-laravel/tests.yml?branch=main&label=tests)](https://github.com/kundan-in/clickhouse-laravel/actions)
[![Total Downloads](https://img.shields.io/packagist/dt/kundan-in/clickhouse-laravel.svg?style=flat-square)](https://packagist.org/packages/kundan-in/clickhouse-laravel)

A comprehensive Laravel database driver for ClickHouse that seamlessly integrates ClickHouse into your Laravel applications with full Eloquent ORM support.

## Features

- **Full Laravel Integration**: Works with Laravel 8, 9, 10, 11, and 12
- **Eloquent ORM Support**: Use familiar Laravel Eloquent syntax with ClickHouse
- **Query Builder**: Comprehensive query builder with ClickHouse-specific features
- **Schema Builder**: Create and manage ClickHouse tables and schemas
- **Custom Casts**: Built-in casts for ClickHouse Array and JSON types
- **Multiple Engines**: Support for all ClickHouse engines (MergeTree, ReplacingMergeTree, etc.)
- **ClickHouse Features**: SAMPLE, FINAL, PREWHERE, and other ClickHouse-specific operations
- **Production Ready**: Comprehensive error handling, security, and performance optimizations
- **Facade Support**: Easy access through Laravel facades
- **Health Checks**: Built-in connection health monitoring

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

### Using Eloquent Models

Create a ClickHouse model by extending `ClickHouseModel`:

```php
<?php

namespace App\Models;

use KundanIn\ClickHouseLaravel\Database\ClickHouseModel;

class AnalyticsEvent extends ClickHouseModel
{
    protected $connection = 'clickhouse';
    protected $table = 'analytics_events';
    
    // Disable Laravel timestamps if not using them
    public $timestamps = false;
    
    // Define fillable attributes
    protected $fillable = [
        'user_id',
        'event_name',
        'properties',
        'created_at'
    ];
    
    // Cast ClickHouse types
    protected function casts(): array
    {
        return [
            'properties' => \KundanIn\ClickHouseLaravel\Casts\ClickHouseJson::class,
            'tags' => \KundanIn\ClickHouseLaravel\Casts\ClickHouseArray::class,
        ];
    }
}
```

### Basic Operations

```php
// Insert data
AnalyticsEvent::create([
    'user_id' => 1,
    'event_name' => 'page_view',
    'properties' => ['page' => '/dashboard', 'referrer' => 'google.com'],
    'created_at' => now()
]);

// Query data
$events = AnalyticsEvent::where('user_id', 1)
    ->whereDate('created_at', today())
    ->get();

// Use ClickHouse-specific features
$sample = AnalyticsEvent::sample(0.1) // 10% sample
    ->where('event_name', 'purchase')
    ->final() // Use FINAL keyword
    ->get();

// Aggregations
$stats = AnalyticsEvent::selectRaw('
    count() as total_events,
    uniq(user_id) as unique_users,
    countIf(event_name = \'purchase\') as purchases
')->first();
```

### Using the Facade

```php
use KundanIn\ClickHouseLaravel\Facades\ClickHouse;

// Raw queries
$results = ClickHouse::select('SELECT * FROM analytics_events WHERE user_id = ?', [1]);

// Health check
if (ClickHouse::healthCheck()) {
    echo "ClickHouse is healthy!";
}

// Server version
echo "ClickHouse version: " . ClickHouse::getServerVersion();

// Query builder
$data = ClickHouse::table('analytics_events')
    ->where('created_at', '>=', today())
    ->groupBy('event_name')
    ->selectRaw('event_name, count() as total')
    ->get();
```

### Schema Management

```php
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

// Create table with ClickHouse engine
Schema::connection('clickhouse')->create('analytics_events', function (Blueprint $table) {
    $table->clickHouseEngine('MergeTree');
    $table->uint64('user_id');
    $table->string('event_name');
    $table->clickHouseJson('properties');
    $table->clickHouseArray('tags', 'String');
    $table->dateTime('created_at');
    
    // ClickHouse specific options
    $table->orderBy(['created_at', 'user_id']);
    $table->partitionBy('toYYYYMM(created_at)');
});
```

### ClickHouse-Specific Query Builder Methods

```php
// SAMPLE clause
$query->sample(0.1); // 10% sample

// FINAL keyword for ReplacingMergeTree
$query->final();

// PREWHERE clause (more efficient than WHERE for some queries)
$query->prewhere('user_id', '>', 1000);

// Array operations
$query->whereArrayHas('tags', 'premium');

// ClickHouse aggregation functions
$query->selectRaw('uniq(user_id) as unique_users');
$query->selectRaw('quantile(0.95)(response_time) as p95_response_time');
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