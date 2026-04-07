<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * Tests for SAMPLE, FINAL, and PREWHERE compilation in the grammar.
 */
class ClickHouseGrammarCompileSelectTest extends TestCase
{
    protected ClickHouseConnection $connection;

    protected function setUp(): void
    {
        parent::setUp();

        $this->connection = new ClickHouseConnection([
            'host' => '127.0.0.1',
            'port' => 8123,
            'username' => 'default',
            'password' => '',
            'database' => 'test_db',
        ]);
    }

    public function test_final_is_injected_after_table(): void
    {
        $sql = $this->connection->table('events')->final()->toSql();

        $this->assertStringContainsString('FINAL', $sql);
        $this->assertMatchesRegularExpression('/"events"\s+FINAL/', $sql);
    }

    public function test_sample_is_injected_after_table(): void
    {
        $sql = $this->connection->table('events')->sample(0.1)->toSql();

        $this->assertStringContainsString('SAMPLE 0.1', $sql);
    }

    public function test_final_and_sample_together(): void
    {
        $sql = $this->connection->table('events')->final()->sample(0.5)->toSql();

        $this->assertStringContainsString('FINAL', $sql);
        $this->assertStringContainsString('SAMPLE 0.5', $sql);

        // FINAL should come before SAMPLE
        $finalPos = strpos($sql, 'FINAL');
        $samplePos = strpos($sql, 'SAMPLE');
        $this->assertLessThan($samplePos, $finalPos);
    }

    public function test_prewhere_is_injected_before_where(): void
    {
        $sql = $this->connection->table('events')
            ->prewhere('date', '>=', '2024-01-01')
            ->where('status', 'active')
            ->toSql();

        $this->assertStringContainsString('PREWHERE', $sql);

        // PREWHERE should come before WHERE
        $prewherePos = stripos($sql, 'PREWHERE');
        $wherePos = stripos($sql, ' where ');
        $this->assertLessThan($wherePos, $prewherePos);
    }

    public function test_prewhere_without_where(): void
    {
        $sql = $this->connection->table('events')
            ->prewhere('date', '>=', '2024-01-01')
            ->toSql();

        $this->assertStringContainsString('PREWHERE', $sql);
        $this->assertStringContainsString('"date"', $sql);
    }

    public function test_all_clickhouse_clauses_together(): void
    {
        $sql = $this->connection->table('events')
            ->final()
            ->sample(0.1)
            ->prewhere('date', '>=', '2024-01-01')
            ->where('status', 'active')
            ->limit(100)
            ->toSql();

        $this->assertStringContainsString('FINAL', $sql);
        $this->assertStringContainsString('SAMPLE 0.1', $sql);
        $this->assertStringContainsString('PREWHERE', $sql);
        $this->assertStringContainsString('LIMIT 100', $sql);
    }

    public function test_array_has_where_clause(): void
    {
        $sql = $this->connection->table('events')
            ->whereArrayHas('tags', 'urgent')
            ->toSql();

        $this->assertStringContainsString('has(', $sql);
        $this->assertStringContainsString('"tags"', $sql);
    }

    public function test_array_has_any_where_clause(): void
    {
        $sql = $this->connection->table('events')
            ->whereArrayHasAny('tags', ['a', 'b'])
            ->toSql();

        $this->assertStringContainsString('hasAny(', $sql);
        $this->assertStringContainsString('"tags"', $sql);
    }

    public function test_array_has_all_where_clause(): void
    {
        $sql = $this->connection->table('events')
            ->whereArrayHasAll('tags', ['x', 'y'])
            ->toSql();

        $this->assertStringContainsString('hasAll(', $sql);
        $this->assertStringContainsString('"tags"', $sql);
    }

    public function test_where_time_uses_format_date_time(): void
    {
        $sql = $this->connection->table('events')
            ->whereTime('created_at', '14:30:00')
            ->toSql();

        $this->assertStringContainsString('formatDateTime(', $sql);
        $this->assertStringContainsString('"created_at"', $sql);
    }

    public function test_truncate_compiles_correctly(): void
    {
        $grammar = $this->connection->getQueryGrammar();
        $builder = $this->connection->table('events');

        $result = $grammar->compileTruncate($builder);

        $this->assertIsArray($result);
        $key = array_key_first($result);
        $this->assertStringContainsStringIgnoringCase('TRUNCATE TABLE', $key);
    }
}
