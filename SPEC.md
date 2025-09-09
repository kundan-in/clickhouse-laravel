# ClickHouse Laravel Package Specification

## Overview

The `kundan-in/clickhouse-laravel` package is a comprehensive Laravel database driver that seamlessly integrates ClickHouse into Laravel applications with full Eloquent ORM support. It provides a production-ready solution for using ClickHouse as an OLAP database alongside traditional Laravel applications.

## Package Metadata

- **Name**: kundan-in/clickhouse-laravel
- **Type**: Library (Composer package)
- **License**: MIT
- **Author**: Kundan (https://github.com/kundan-in)
- **Repository**: https://github.com/kundan-in/clickhouse-laravel
- **Keywords**: laravel, clickhouse, database, analytics, olap, big-data

## Technical Requirements

### PHP & Laravel Compatibility
- **PHP**: ^8.1|^8.2|^8.3
- **Laravel Framework**: ^8.0|^9.0|^10.0|^11.0|^12.0
- **Laravel Support**: ^8.0|^9.0|^10.0|^11.0|^12.0

### Dependencies
- **Primary**: `smi2/phpclickhouse`: ^1.6 (ClickHouse client for PHP)
- **Dev Dependencies**:
  - `orchestra/testbench`: ^8.0|^9.0
  - `phpunit/phpunit`: ^10.0|^11.0
  - `mockery/mockery`: ^1.6

## Architecture

### Core Components

#### 1. Service Provider (`ClickHouseServiceProvider`)
- **Location**: `src/ClickHouseServiceProvider.php`
- **Purpose**: Registers ClickHouse database driver with Laravel's DatabaseManager
- **Features**:
  - Auto-discovery through Laravel's package discovery
  - Configuration publishing
  - Database driver registration
  - Facade binding

#### 2. Database Connection (`ClickHouseConnection`)
- **Location**: `src/Database/ClickHouseConnection.php`
- **Purpose**: Extends Laravel's Connection class for ClickHouse connectivity
- **Features**:
  - Uses `smi2/phpClickHouse` client internally
  - Implements ConnectionInterface
  - Handles parameter binding and SQL injection prevention
  - Automatic database prefixing
  - Health check capabilities
  - Result normalization for Laravel compatibility

#### 3. Query Builder (`ClickHouseQueryBuilder`)
- **Location**: `src/Database/ClickHouseQueryBuilder.php`
- **Purpose**: Extends Laravel's Query Builder with ClickHouse-specific features
- **ClickHouse-Specific Methods**:
  - `sample(float $ratio)`: SAMPLE clause for sampling data
  - `final()`: FINAL keyword for ReplacingMergeTree engines
  - `prewhere()`: PREWHERE optimization clause
  - `whereArrayHas()`: Array operations support
  - `uniq()`, `uniqExact()`: ClickHouse aggregation functions
  - `groupByWithRollup()`, `groupByWithCube()`: Advanced grouping

#### 4. Query Grammar (`ClickHouseQueryGrammar`)
- **Location**: `src/Database/ClickHouseQueryGrammar.php`
- **Purpose**: Translates Laravel query builder to ClickHouse SQL syntax
- **Features**:
  - ClickHouse-specific SQL compilation
  - Array operation support
  - Date/time function mapping
  - Aggregate function support

#### 5. Eloquent Model (`ClickHouseModel`)
- **Location**: `src/Database/ClickHouseModel.php`
- **Purpose**: Base model class for ClickHouse tables
- **Features**:
  - Extends Laravel's Eloquent Model
  - Uses ClickHouseEloquentBuilder
  - Soft delete simulation (flag-based)
  - Relationship support (optimized for ClickHouse)
  - Custom casting methods

#### 6. Eloquent Builder (`ClickHouseEloquentBuilder`)
- **Location**: `src/Database/ClickHouseEloquentBuilder.php`
- **Purpose**: Eloquent query builder with ClickHouse optimizations
- **Features**:
  - Extends Laravel's Eloquent Builder
  - ClickHouse-specific query methods
  - Proper method chaining

#### 7. Schema Components
- **ClickHouseSchemaBuilder**: `src/Database/ClickHouseSchemaBuilder.php`
- **ClickHouseSchemaGrammar**: `src/Database/ClickHouseSchemaGrammar.php`
- **ClickHouseBlueprint**: `src/Database/ClickHouseBlueprint.php`

### Custom Casts

#### ClickHouseArray Cast
- **Location**: `src/Casts/ClickHouseArray.php`
- **Purpose**: Handles ClickHouse Array type conversion
- **Usage**: `'column_name' => ClickHouseArray::class . ':String'`

#### ClickHouseJson Cast
- **Location**: `src/Casts/ClickHouseJson.php`
- **Purpose**: Handles JSON data type conversion
- **Usage**: `'column_name' => ClickHouseJson::class`

### Facade Support
- **Location**: `src/Facades/ClickHouse.php`
- **Purpose**: Provides convenient static access to ClickHouse connection
- **Alias**: `ClickHouse` (auto-registered)

### Exception Handling
- **ClickHouseException**: `src/Exceptions/ClickHouseException.php`
- **UnsupportedOperationException**: `src/Exceptions/UnsupportedOperationException.php`

## Configuration

### Environment Variables
```env
CLICKHOUSE_HOST=127.0.0.1
CLICKHOUSE_PORT=8123
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=default
CLICKHOUSE_READONLY=0
CLICKHOUSE_MAX_EXECUTION_TIME=60
```

### Configuration File
- **Location**: `config/clickhouse.php`
- **Publishing**: `php artisan vendor:publish --provider="KundanIn\ClickHouseLaravel\ClickHouseServiceProvider" --tag="clickhouse-config"`

### Database Connection Configuration
Add to `config/database.php`:
```php
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
```

## Usage Patterns

### Model Definition
```php
use KundanIn\ClickHouseLaravel\Database\ClickHouseModel;

class AnalyticsEvent extends ClickHouseModel
{
    protected $connection = 'clickhouse';
    protected $table = 'analytics_events';
    public $timestamps = false;
    
    protected $fillable = [
        'user_id',
        'event_name',
        'properties',
        'created_at'
    ];
    
    protected function casts(): array
    {
        return [
            'properties' => \KundanIn\ClickHouseLaravel\Casts\ClickHouseJson::class,
            'tags' => \KundanIn\ClickHouseLaravel\Casts\ClickHouseArray::class,
        ];
    }
}
```

### Query Operations
```php
// Basic operations
AnalyticsEvent::create([...]);
$events = AnalyticsEvent::where('user_id', 1)->get();

// ClickHouse-specific features
$sample = AnalyticsEvent::sample(0.1)->final()->get();
$stats = AnalyticsEvent::selectRaw('count() as total, uniq(user_id) as unique_users')->first();
```

### Facade Usage
```php
use KundanIn\ClickHouseLaravel\Facades\ClickHouse;

$results = ClickHouse::select('SELECT * FROM analytics_events WHERE user_id = ?', [1]);
$healthy = ClickHouse::healthCheck();
$version = ClickHouse::getServerVersion();
```

## Testing Framework

### Test Structure
- **Base**: `tests/TestCase.php` - Orchestra Testbench setup
- **Unit Tests**: `tests/Unit/` - Component-specific tests
- **Feature Tests**: `tests/Feature/` - Integration tests
- **Configuration**: `phpunit.xml` - PHPUnit configuration

### Test Categories
1. **Connection Tests**: Database connectivity and parameter binding
2. **Query Builder Tests**: SQL generation and method chaining
3. **Model Tests**: Eloquent operations and relationships
4. **Grammar Tests**: SQL compilation accuracy
5. **Cast Tests**: Data type conversion
6. **Service Provider Tests**: Laravel integration

### Test Environment
- Uses Orchestra Testbench for Laravel environment simulation
- Mock ClickHouse client for isolated testing
- Configurable test database connection
- Coverage reporting enabled

## ClickHouse-Specific Features

### Supported Operations
- **SAMPLE**: Data sampling for performance
- **FINAL**: Deduplication for ReplacingMergeTree
- **PREWHERE**: Optimized filtering
- **Array Operations**: has, hasAny, hasAll functions
- **Advanced Aggregations**: uniq, quantile, topK functions
- **GROUP BY Extensions**: WITH ROLLUP, WITH CUBE

### Engine Support
- MergeTree family engines
- ReplacingMergeTree (with FINAL support)
- Custom engine specification in schema builder

### Limitations
- **Transactions**: Limited support (ClickHouse limitation)
- **Joins**: Only INNER and LEFT joins supported
- **Updates/Deletes**: Uses ALTER TABLE syntax
- **Primary Keys**: Different semantics from traditional RDBMS

## Security Features

### SQL Injection Prevention
- Comprehensive parameter binding
- Value escaping for all data types
- Query validation and sanitization

### Access Control
- Read-only connection support
- Configurable execution time limits
- User-based authentication

## Performance Optimizations

### Connection Management
- Persistent connections
- Connection pooling support
- Health check monitoring

### Query Optimization
- Automatic database prefixing
- Result normalization caching
- Efficient array handling

### Memory Management
- Result set streaming for large datasets
- Lazy loading support
- Memory-efficient data processing

## Monitoring & Debugging

### Health Checks
- Connection status monitoring
- Server version detection
- Query execution tracking

### Error Handling
- Comprehensive exception hierarchy
- Detailed error reporting
- Graceful failure handling

## Upgrade & Migration Path

### Version Compatibility
- Backward compatibility with existing Laravel applications
- Smooth upgrade path between Laravel versions
- Configuration migration support

### Database Migrations
- Schema builder for ClickHouse-specific features
- Migration file compatibility
- Index and partition management

## Future Enhancements

### Planned Features
1. **Advanced Materialized Views**: Support for ClickHouse materialized views
2. **Cluster Support**: Multi-node ClickHouse cluster operations
3. **Real-time Streaming**: Integration with ClickHouse streaming capabilities
4. **Performance Analytics**: Built-in query performance monitoring
5. **Advanced Caching**: Result caching for frequently accessed data

### Extension Points
- Custom query grammar extensions
- Additional cast types
- Custom connection drivers
- Plugin architecture for third-party integrations

## Contributing Guidelines

### Development Setup
1. Clone repository
2. Install dependencies: `composer install`
3. Run tests: `composer test`
4. Check coverage: `composer test-coverage`

### Code Standards
- PSR-4 autoloading
- Laravel coding standards
- PHPDoc documentation
- Comprehensive testing required

### Testing Requirements
- Unit tests for all new features
- Integration tests for complex functionality
- Backward compatibility testing
- Performance regression testing

## Deployment Considerations

### Production Readiness
- Comprehensive error handling
- Security best practices
- Performance optimizations
- Monitoring capabilities

### Scaling Considerations
- Connection pool management
- Query optimization strategies
- Resource usage monitoring
- Cluster deployment support

This specification serves as the complete reference for the ClickHouse Laravel package, covering all aspects from architecture to usage patterns. It should be updated as new features are added or existing functionality is modified to ensure the package remains well-documented and maintainable.