<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use ReflectionMethod;

/**
 * Test ClickHouse connection data normalization
 */
class ClickHouseConnectionNormalizationTest extends TestCase
{
    /**
     * Test normalization of complex ClickHouse results
     */
    public function test_normalize_results_handles_complex_data()
    {
        $connection = new ClickHouseConnection([
            'host' => '127.0.0.1',
            'port' => 8123,
            'database' => 'test_db',
            'username' => 'default',
            'password' => '',
        ]);

        // Use reflection to access the protected method
        $reflection = new ReflectionMethod($connection, 'normalizeResults');
        $reflection->setAccessible(true);

        // Test data that might come from ClickHouse
        $clickhouseResults = [
            [
                'id' => 1,
                'name' => 'John',
                'tags' => ['php', 'laravel', 'clickhouse'], // Simple array
                'metadata' => [ // Complex nested data
                    'profile' => ['age' => 30, 'city' => 'NYC'],
                    'settings' => ['theme' => 'dark'],
                ],
                'config' => (object) ['debug' => true, 'cache' => false], // Object
            ],
            [
                'id' => 2,
                'name' => 'Jane',
                'tags' => ['vue', 'nuxt'], // Simple array
                'metadata' => ['profile' => ['age' => 25]], // Complex nested
                'config' => (object) ['debug' => false], // Object
            ],
        ];

        $normalized = $reflection->invoke($connection, $clickhouseResults);

        // Verify structure is maintained
        $this->assertCount(2, $normalized);
        $this->assertEquals(1, $normalized[0]['id']);
        $this->assertEquals('John', $normalized[0]['name']);
        $this->assertEquals(2, $normalized[1]['id']);
        $this->assertEquals('Jane', $normalized[1]['name']);

        // Verify arrays and objects are converted to JSON strings
        $this->assertIsString($normalized[0]['tags']);
        $this->assertIsString($normalized[0]['metadata']);
        $this->assertIsString($normalized[0]['config']);

        // Verify JSON is valid and correct
        $this->assertEquals(['php', 'laravel', 'clickhouse'], json_decode($normalized[0]['tags'], true));
        $expectedMetadata = ['profile' => ['age' => 30, 'city' => 'NYC'], 'settings' => ['theme' => 'dark']];
        $this->assertEquals($expectedMetadata, json_decode($normalized[0]['metadata'], true));
        $this->assertEquals(['debug' => true, 'cache' => false], json_decode($normalized[0]['config'], true));
    }

    /**
     * Test normalization preserves scalar values
     */
    public function test_normalize_results_preserves_scalar_values()
    {
        $connection = new ClickHouseConnection([
            'host' => '127.0.0.1',
            'port' => 8123,
            'database' => 'test_db',
            'username' => 'default',
            'password' => '',
        ]);

        $reflection = new ReflectionMethod($connection, 'normalizeResults');
        $reflection->setAccessible(true);

        $clickhouseResults = [
            [
                'id' => 42,
                'name' => 'Test User',
                'active' => true,
                'score' => 98.5,
                'created_at' => '2024-01-01 12:00:00',
            ],
        ];

        $normalized = $reflection->invoke($connection, $clickhouseResults);

        // Verify scalar values remain unchanged
        $this->assertSame(42, $normalized[0]['id']);
        $this->assertSame('Test User', $normalized[0]['name']);
        $this->assertSame(true, $normalized[0]['active']);
        $this->assertSame(98.5, $normalized[0]['score']);
        $this->assertSame('2024-01-01 12:00:00', $normalized[0]['created_at']);
    }
}
