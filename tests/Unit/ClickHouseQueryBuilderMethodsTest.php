<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use InvalidArgumentException;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Database\ClickHouseQueryBuilder;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * Tests for ClickHouse-specific query builder methods.
 */
class ClickHouseQueryBuilderMethodsTest extends TestCase
{
    protected ClickHouseQueryBuilder $builder;

    protected function setUp(): void
    {
        parent::setUp();

        $connection = new ClickHouseConnection([
            'host' => '127.0.0.1',
            'port' => 8123,
            'username' => 'default',
            'password' => '',
            'database' => 'test_db',
        ]);

        $this->builder = $connection->query();
    }

    public function test_sample_sets_ratio(): void
    {
        $builder = $this->builder->from('events')->sample(0.1);

        $this->assertEquals(0.1, $builder->sample);
    }

    public function test_sample_rejects_invalid_ratio(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->builder->from('events')->sample(0);
    }

    public function test_sample_rejects_ratio_above_one(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->builder->from('events')->sample(1.5);
    }

    public function test_final_sets_flag(): void
    {
        $builder = $this->builder->from('events')->final();

        $this->assertTrue($builder->final);
    }

    public function test_prewhere_adds_condition(): void
    {
        $builder = $this->builder->from('events')->prewhere('user_id', '=', 123);

        $this->assertCount(1, $builder->prewhere);
        $this->assertEquals('user_id', $builder->prewhere[0]['column']);
        $this->assertEquals('=', $builder->prewhere[0]['operator']);
        $this->assertEquals(123, $builder->prewhere[0]['value']);
    }

    public function test_where_array_has(): void
    {
        $builder = $this->builder->from('events')->whereArrayHas('tags', 'important');

        $this->assertCount(1, $builder->wheres);
        $this->assertEquals('ArrayHas', $builder->wheres[0]['type']);
        $this->assertEquals('tags', $builder->wheres[0]['column']);
        $this->assertEquals('important', $builder->wheres[0]['value']);
    }

    public function test_where_array_has_any(): void
    {
        $builder = $this->builder->from('events')->whereArrayHasAny('tags', ['a', 'b']);

        $this->assertCount(1, $builder->wheres);
        $this->assertEquals('ArrayHasAny', $builder->wheres[0]['type']);
        $this->assertEquals(['a', 'b'], $builder->wheres[0]['values']);
    }

    public function test_where_array_has_all(): void
    {
        $builder = $this->builder->from('events')->whereArrayHasAll('tags', ['x', 'y']);

        $this->assertCount(1, $builder->wheres);
        $this->assertEquals('ArrayHasAll', $builder->wheres[0]['type']);
        $this->assertEquals(['x', 'y'], $builder->wheres[0]['values']);
    }

    public function test_group_by_with_rollup(): void
    {
        $sql = $this->builder->from('events')
            ->select($this->builder->raw('status, count(*) as cnt'))
            ->groupByWithRollup('status')
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('WITH ROLLUP', $sql);
    }

    public function test_group_by_with_cube(): void
    {
        $sql = $this->builder->from('events')
            ->select($this->builder->raw('status, count(*) as cnt'))
            ->groupByWithCube('status')
            ->toSql();

        $this->assertStringContainsStringIgnoringCase('WITH CUBE', $sql);
    }

    public function test_chaining_clickhouse_specific_methods(): void
    {
        $builder = $this->builder
            ->from('events')
            ->sample(0.5)
            ->final()
            ->prewhere('date', '>=', '2024-01-01')
            ->where('status', 'active')
            ->limit(100);

        $this->assertEquals(0.5, $builder->sample);
        $this->assertTrue($builder->final);
        $this->assertCount(1, $builder->prewhere);
        $this->assertCount(1, $builder->wheres);
        $this->assertEquals(100, $builder->limit);
    }

    public function test_standard_where_date_works(): void
    {
        $sql = $this->builder->from('events')
            ->whereDate('created_at', '2025-09-07')
            ->toSql();

        $this->assertStringContainsString('toDate(', $sql);
        $this->assertStringContainsString('"created_at"', $sql);
    }

    public function test_standard_where_year_works(): void
    {
        $sql = $this->builder->from('events')
            ->whereYear('created_at', 2025)
            ->toSql();

        $this->assertStringContainsString('toYear(', $sql);
    }

    public function test_standard_where_month_works(): void
    {
        $sql = $this->builder->from('events')
            ->whereMonth('created_at', 9)
            ->toSql();

        $this->assertStringContainsString('toMonth(', $sql);
    }
}
