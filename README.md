# ClickHouse Laravel

[![Tests](https://github.com/kundan-in/clickhouse-laravel/actions/workflows/tests.yml/badge.svg)](https://github.com/kundan-in/clickhouse-laravel/actions)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/kundan-in/clickhouse-laravel.svg)](https://packagist.org/packages/kundan-in/clickhouse-laravel)
[![PHP Version](https://img.shields.io/packagist/php-v/kundan-in/clickhouse-laravel.svg)](https://packagist.org/packages/kundan-in/clickhouse-laravel)
[![License](https://img.shields.io/packagist/l/kundan-in/clickhouse-laravel.svg)](https://packagist.org/packages/kundan-in/clickhouse-laravel)

A production-ready ClickHouse database driver for Laravel with full Eloquent ORM support. Use ClickHouse the same way you use MySQL in Laravel.

## Table of Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Quick Start](#quick-start)
- [Query Builder](#query-builder)
- [ClickHouse-Specific Features](#clickhouse-specific-features)
- [Schema & Migrations](#schema--migrations)
- [Eloquent Model](#eloquent-model)
- [Batch Insert](#batch-insert)
- [Feature Comparison](#feature-comparison)
- [Troubleshooting](#troubleshooting)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Requirements

- PHP 8.1 or higher
- Laravel 8.x through 13.x
- ClickHouse server (any recent version)

## Installation

```bash
composer require kundan-in/clickhouse-laravel
```

Publish the configuration file:

```bash
php artisan vendor:publish --provider="KundanIn\ClickHouseLaravel\ClickHouseServiceProvider" --tag="clickhouse-config"
```

## Configuration

Add your ClickHouse connection to `config/database.php`:

```php
'connections' => [
    // ... other connections

    'clickhouse' => [
        'driver'          => 'clickhouse',
        'host'            => env('CLICKHOUSE_HOST', '127.0.0.1'),
        'port'            => env('CLICKHOUSE_PORT', 8123),
        'username'        => env('CLICKHOUSE_USERNAME', 'default'),
        'password'        => env('CLICKHOUSE_PASSWORD', ''),
        'database'        => env('CLICKHOUSE_DATABASE', 'default'),
        'timeout'         => env('CLICKHOUSE_TIMEOUT', 120),
        'connect_timeout' => env('CLICKHOUSE_CONNECT_TIMEOUT', 5),
        'settings'        => [
            'readonly'           => env('CLICKHOUSE_READONLY', 0),
            'max_execution_time' => env('CLICKHOUSE_MAX_EXECUTION_TIME', 60),
        ],
    ],
],
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `127.0.0.1` | ClickHouse server hostname |
| `CLICKHOUSE_PORT` | `8123` | HTTP interface port |
| `CLICKHOUSE_USERNAME` | `default` | Authentication username |
| `CLICKHOUSE_PASSWORD` | _(empty)_ | Authentication password |
| `CLICKHOUSE_DATABASE` | `default` | Default database |
| `CLICKHOUSE_TIMEOUT` | `120` | Request timeout in seconds |
| `CLICKHOUSE_CONNECT_TIMEOUT` | `5` | TCP connection timeout in seconds |
| `CLICKHOUSE_READONLY` | `0` | Read-only mode (0=off, 1=on) |
| `CLICKHOUSE_MAX_EXECUTION_TIME` | `60` | Server-side query timeout in seconds |

## Quick Start

### Create a Model

```php
use KundanIn\ClickHouseLaravel\Database\ClickHouseModel;

class AnalyticsEvent extends ClickHouseModel
{
    protected $connection = 'clickhouse';
    protected $table = 'analytics_events';
    public $timestamps = false;

    protected $fillable = [
        'user_id', 'event_name', 'properties', 'created_at',
    ];

    protected function casts(): array
    {
        return [
            'properties' => \KundanIn\ClickHouseLaravel\Casts\ClickHouseJson::class,
        ];
    }
}
```

### Basic Operations

```php
// Retrieve records
$events = AnalyticsEvent::where('user_id', 123)->limit(10)->get();
$event  = AnalyticsEvent::where('event_name', 'page_view')->first();
$count  = AnalyticsEvent::count();

// Insert
AnalyticsEvent::create([
    'user_id'    => 123,
    'event_name' => 'page_view',
    'created_at' => now(),
]);

// Aggregations
$total = AnalyticsEvent::sum('duration');
$avg   = AnalyticsEvent::avg('duration');
$max   = AnalyticsEvent::max('duration');
```

## Query Builder

All standard Laravel query builder methods work:

```php
use Illuminate\Support\Facades\DB;

// Where clauses
DB::connection('clickhouse')->table('events')
    ->where('status', 'active')
    ->where('score', '>', 80)
    ->whereIn('type', ['click', 'view'])
    ->whereBetween('created_at', ['2024-01-01', '2024-12-31'])
    ->whereNotNull('session_id')
    ->limit(100)
    ->get();

// Aggregations with grouping
DB::connection('clickhouse')->table('events')
    ->selectRaw('device_type, count() as total, avg(duration) as avg_duration')
    ->groupBy('device_type')
    ->having('total', '>', 100)
    ->orderByRaw('total DESC')
    ->get();

// Joins
DB::connection('clickhouse')->table('events')
    ->join('users', 'events.user_id', '=', 'users.id')
    ->select('events.*', 'users.name')
    ->get();

// Subqueries, raw expressions, pagination
DB::connection('clickhouse')->table('events')
    ->whereRaw('toDate(created_at) = today()')
    ->pluck('event_name');
```

## ClickHouse-Specific Features

### SAMPLE - Approximate Queries

```php
// Query only 10% of the data (requires SAMPLE BY in table definition)
AnalyticsEvent::query()->sample(0.1)->count();
```

### FINAL - Deduplicated Reads

```php
// Force merge for ReplacingMergeTree tables
AnalyticsEvent::query()->final()->where('user_id', 123)->get();
```

### PREWHERE - I/O Optimization

```php
// Filter before reading full columns (reduces disk I/O)
AnalyticsEvent::query()
    ->prewhere('date', '>=', '2024-01-01')
    ->where('status', 'active')
    ->get();
```

### Array Operations

```php
// Check if array column contains a value
AnalyticsEvent::query()->whereArrayHas('tags', 'important')->get();

// Check if array has any of the given values
AnalyticsEvent::query()->whereArrayHasAny('tags', ['urgent', 'important'])->get();

// Check if array has all of the given values
AnalyticsEvent::query()->whereArrayHasAll('tags', ['reviewed', 'approved'])->get();
```

### Advanced Grouping

```php
AnalyticsEvent::query()
    ->selectRaw('device_type, browser, count() as cnt')
    ->groupByWithRollup('device_type', 'browser')
    ->get();

AnalyticsEvent::query()
    ->selectRaw('device_type, browser, count() as cnt')
    ->groupByWithCube('device_type', 'browser')
    ->get();
```

### ClickHouse Aggregation Functions

```php
// Approximate distinct count (fast)
$approxUnique = DB::connection('clickhouse')->table('events')->uniq('user_id');

// Exact distinct count
$exactUnique = DB::connection('clickhouse')->table('events')->uniqExact('user_id');
```

## Schema & Migrations

### Creating Tables

```php
use Illuminate\Support\Facades\Schema;
use KundanIn\ClickHouseLaravel\Database\ClickHouseBlueprint;

Schema::connection('clickhouse')->create('analytics_events', function (ClickHouseBlueprint $table) {
    $table->uint64('id');
    $table->string('event_name');
    $table->uint32('user_id');
    $table->float64('duration');
    $table->array('tags', 'String');
    $table->lowCardinality('device_type', 'String');
    $table->dateTime64('created_at', 3);

    $table->engine('MergeTree');
    $table->orderBy(['id', 'created_at']);
    $table->partitionBy('toYYYYMM(created_at)');
    $table->ttl('created_at + INTERVAL 90 DAY');
    $table->settings(['index_granularity' => 8192]);
});
```

### Engine Types

```php
// ReplacingMergeTree (deduplication)
$builder = Schema::connection('clickhouse')->getSchemaBuilder();
$builder->createReplacingMergeTree('events', function ($table) {
    $table->uint64('id');
    $table->uint32('version');
    $table->string('data');
    $table->orderBy('id');
}, 'version');

// SummingMergeTree (automatic aggregation)
$builder->createSummingMergeTree('daily_stats', function ($table) {
    $table->date('date');
    $table->string('page');
    $table->uint64('views');
    $table->orderBy(['date', 'page']);
}, ['views']);

// CollapsingMergeTree (row versioning)
$builder->createCollapsingMergeTree('sessions', function ($table) {
    $table->uint64('user_id');
    $table->dateTime('started_at');
    $table->int8('sign');
    $table->orderBy('user_id');
}, 'sign');
```

### Available Column Types

| Method | ClickHouse Type | Description |
|--------|----------------|-------------|
| `int8()` / `int16()` / `int32()` / `int64()` | Int8-64 | Signed integers |
| `uint8()` / `uint16()` / `uint32()` / `uint64()` | UInt8-64 | Unsigned integers |
| `float32()` / `float64()` | Float32/64 | Floating point |
| `decimal($p, $s)` | Decimal(P, S) | Fixed-point decimal |
| `string()` | String | Variable-length string |
| `fixedString($n)` | FixedString(N) | Fixed-length string |
| `uuid()` | UUID | UUID type |
| `date()` | Date | Calendar date |
| `dateTime()` | DateTime | Date and time |
| `dateTime64($precision)` | DateTime64(P) | High-precision datetime |
| `boolean()` | UInt8 | Boolean (0/1) |
| `array($col, $type)` | Array(T) | Array of elements |
| `tuple($col, $types)` | Tuple(T...) | Fixed-size tuple |
| `map($col, $k, $v)` | Map(K, V) | Key-value map |
| `nested($col, $struct)` | Nested(...) | Nested structure |
| `enum8($col, $vals)` / `enum16()` | Enum8/16 | Enumeration |
| `lowCardinality($col, $type)` | LowCardinality(T) | Dictionary encoding |
| `nullableColumn($col, $type)` | Nullable(T) | Nullable wrapper |

### Materialized Views

```php
$builder = Schema::connection('clickhouse')->getSchemaBuilder();

$builder->createMaterializedView(
    'events_daily',
    'SELECT toDate(created_at) as day, count() as cnt FROM events GROUP BY day',
    'events_daily_agg'
);

$builder->dropMaterializedView('events_daily');
```

## Eloquent Model

### Custom Casts

```php
use KundanIn\ClickHouseLaravel\Casts\ClickHouseArray;
use KundanIn\ClickHouseLaravel\Casts\ClickHouseJson;

class Event extends ClickHouseModel
{
    protected function casts(): array
    {
        return [
            'tags'       => ClickHouseArray::class . ':String',
            'properties' => ClickHouseJson::class,
        ];
    }
}
```

### Soft Deletes

Use Laravel's `SoftDeletes` trait as normal. The driver compiles DELETE to ClickHouse's `ALTER TABLE ... DELETE` syntax:

```php
use Illuminate\Database\Eloquent\SoftDeletes;

class Event extends ClickHouseModel
{
    use SoftDeletes;
}
```

### Facade

```php
use KundanIn\ClickHouseLaravel\Facades\ClickHouse;

$results = ClickHouse::select('SELECT count() as cnt FROM events');
$healthy = ClickHouse::healthCheck();
$version = ClickHouse::getServerVersion();
```

## Batch Insert

For high-throughput data loading, use `bulkInsert()` which uses ClickHouse's native columnar format:

```php
$connection = DB::connection('clickhouse');

$rows = [
    ['user_id' => 1, 'event' => 'click', 'created_at' => '2024-01-01 00:00:00'],
    ['user_id' => 2, 'event' => 'view',  'created_at' => '2024-01-01 00:00:01'],
    // ... thousands more rows
];

$connection->bulkInsert('events', $rows);
```

This is significantly faster than individual `INSERT` statements for large datasets.

## Feature Comparison

| Feature | MySQL | ClickHouse Laravel |
|---------|-------|--------------------|
| `select` / `get` / `first` / `find` | Yes | Yes |
| `where` / `whereIn` / `whereBetween` | Yes | Yes |
| `whereNull` / `whereNotNull` | Yes | Yes |
| `whereDate` / `whereMonth` / `whereYear` | Yes | Yes (uses ClickHouse functions) |
| `orderBy` / `groupBy` / `having` | Yes | Yes |
| `limit` / `offset` / `skip` / `take` | Yes | Yes |
| `count` / `sum` / `avg` / `min` / `max` | Yes | Yes |
| `pluck` / `value` / `exists` | Yes | Yes |
| `distinct` / `selectRaw` / `whereRaw` | Yes | Yes |
| `join` / `leftJoin` | Yes | Yes |
| `insert` / `create` | Yes | Yes |
| `update` | Yes | Yes (ALTER TABLE UPDATE) |
| `delete` | Yes | Yes (ALTER TABLE DELETE) |
| `cursor` / `lazy` / `chunk` | Yes | Yes |
| `toSql` / `toArray` / `toJson` | Yes | Yes |
| `insertGetId` | Yes | No (no auto-increment) |
| `upsert` | Yes | No (use ReplacingMergeTree) |
| Transactions | Yes | No (ClickHouse limitation) |
| Foreign keys | Yes | No (ClickHouse limitation) |
| `SAMPLE` / `FINAL` / `PREWHERE` | No | Yes |
| Array operations | No | Yes |
| `bulkInsert` | No | Yes |
| `uniq` / `uniqExact` | No | Yes |

## Troubleshooting

### "Too few arguments to Grammar::__construct()"
Ensure you're using v1.4.0+ which supports Laravel 12/13.

### UPDATE/DELETE require WHERE clause
ClickHouse's ALTER TABLE UPDATE/DELETE operations require a WHERE clause for safety. This is enforced by the driver.

### "ClickHouse does not support auto-incrementing IDs"
Use `UUID` columns or application-generated IDs instead of `insertGetId()`.

### Soft deletes not working
Use Laravel's standard `SoftDeletes` trait. The driver handles the `ALTER TABLE ... DELETE` syntax automatically.

### Query timeout
Increase `CLICKHOUSE_TIMEOUT` (HTTP request timeout) and `CLICKHOUSE_MAX_EXECUTION_TIME` (server-side query limit) in your `.env`.

## Testing

```bash
# Run the test suite
composer test

# Run with coverage
composer test-coverage

# Run a specific test
vendor/bin/phpunit --filter=test_name
```

## Contributing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

The MIT License (MIT). Please see [License File](LICENSE) for more information.
