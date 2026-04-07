# Contributing

Thank you for considering contributing to the ClickHouse Laravel package! This document provides guidelines and instructions for contributing.

## Development Setup

1. Fork and clone the repository
2. Install dependencies:
   ```bash
   composer install
   ```
3. Run the test suite:
   ```bash
   composer test
   ```

## Coding Standards

This project follows the [Laravel coding style](https://laravel.com/docs/contributions#coding-style) enforced by [Laravel Pint](https://laravel.com/docs/pint).

Run the formatter before submitting:
```bash
vendor/bin/pint
```

### Guidelines

- Use PHP 8.1+ features (constructor promotion, named arguments, union types)
- Add proper PHPDoc blocks with `@param`, `@return`, `@throws` tags
- Do not use `{@inheritdoc}` — write full docblocks for every method
- Follow existing patterns in the codebase

## Testing

- Every change must include tests
- Use PHPUnit (not Pest)
- Run specific tests: `vendor/bin/phpunit --filter=test_name`
- Run full suite: `composer test`

### Test Structure

- `tests/Unit/` — Unit tests with mocked ClickHouse client
- `tests/Feature/` — Feature tests (not run by default)
- `tests/Integration/` — Integration tests requiring a ClickHouse server

### Writing Tests

- Mock the `ClickHouseDB\Client` using Mockery
- Return `ClickHouseDB\Statement` mocks (not raw arrays) from `Client::select()`
- Use `assertStringContainsStringIgnoringCase()` for SQL keyword assertions
- Create `ClickHouseConnection` instances for grammar/builder tests

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with tests
3. Run `composer test` and `vendor/bin/pint`
4. Update `CHANGELOG.md` under an `[Unreleased]` section
5. Submit a pull request with a clear description

## Reporting Issues

- Use [GitHub Issues](https://github.com/kundan-in/clickhouse-laravel/issues)
- Include: PHP version, Laravel version, ClickHouse version, error message, and minimal reproduction steps
