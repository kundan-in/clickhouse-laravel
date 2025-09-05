<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use Exception;
use KundanIn\ClickHouseLaravel\Database\ClickHouseQueryGrammar;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use Mockery;

/**
 * ClickHouse Query Grammar Test
 *
 * Tests the ClickHouse query grammar functionality and limitations.
 *
 * @package KundanIn\ClickHouseLaravel\Tests\Unit
 */
class ClickHouseQueryGrammarTest extends TestCase
{
    protected ClickHouseQueryGrammar $grammar;

    /**
     * Set up the test environment.
     *
     * @return void
     */
    protected function setUp(): void
    {
        parent::setUp();
        
        $this->grammar = new ClickHouseQueryGrammar();
    }

    /**
     * Test compileLimit method.
     *
     * @return void
     */
    public function test_compile_limit(): void
    {
        $query = Mockery::mock('query');
        
        $result = $this->grammar->compileLimit($query, 10);
        
        $this->assertEquals('LIMIT 10', $result);
    }

    /**
     * Test compileLimit with string input.
     *
     * @return void
     */
    public function test_compile_limit_with_string(): void
    {
        $query = Mockery::mock('query');
        
        $result = $this->grammar->compileLimit($query, '25');
        
        $this->assertEquals('LIMIT 25', $result);
    }

    /**
     * Test compileInsert method.
     *
     * @return void
     */
    public function test_compile_insert(): void
    {
        $query = Mockery::mock('query');
        $query->from = 'events';
        
        $values = [
            ['id' => 1, 'name' => 'test1'],
            ['id' => 2, 'name' => 'test2']
        ];

        $result = $this->grammar->compileInsert($query, $values);
        
        $this->assertStringContainsString('INSERT INTO', $result);
        $this->assertStringContainsString('events', $result);
        $this->assertStringContainsString('id', $result);
        $this->assertStringContainsString('name', $result);
        $this->assertStringContainsString('VALUES', $result);
    }

    /**
     * Test compileInsert with empty values.
     *
     * @return void
     */
    public function test_compile_insert_with_empty_values(): void
    {
        $query = Mockery::mock('query');
        
        $result = $this->grammar->compileInsert($query, []);
        
        $this->assertEquals('', $result);
    }

    /**
     * Test that compileUpdate throws exception.
     *
     * @return void
     */
    public function test_compile_update_throws_exception(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('ClickHouse does not support standard UPDATE queries. Use ALTER TABLE ... UPDATE instead.');
        
        $query = Mockery::mock('query');
        $values = ['name' => 'updated'];
        
        $this->grammar->compileUpdate($query, $values);
    }

    /**
     * Test that compileDelete throws exception.
     *
     * @return void
     */
    public function test_compile_delete_throws_exception(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('ClickHouse does not support standard DELETE queries. Use ALTER TABLE ... DELETE instead.');
        
        $query = Mockery::mock('query');
        
        $this->grammar->compileDelete($query);
    }

    /**
     * Test compileSelect calls parent method.
     *
     * @return void
     */
    public function test_compile_select_calls_parent(): void
    {
        $query = Mockery::mock('query');
        $query->columns = ['*'];
        $query->from = 'events';
        $query->wheres = [];
        $query->groups = [];
        $query->havings = [];
        $query->orders = [];
        $query->limit = null;
        $query->offset = null;
        $query->unions = [];
        $query->lock = null;
        
        $result = $this->grammar->compileSelect($query);
        
        $this->assertIsString($result);
        $this->assertStringContainsString('select', strtolower($result));
    }

    /**
     * Clean up Mockery after each test.
     *
     * @return void
     */
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
}