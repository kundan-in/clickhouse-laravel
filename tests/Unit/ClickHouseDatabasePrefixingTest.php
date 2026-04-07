<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use ClickHouseDB\Client;
use ClickHouseDB\Statement;
use KundanIn\ClickHouseLaravel\Database\ClickHouseConnection;
use KundanIn\ClickHouseLaravel\Tests\TestCase;
use Mockery;

/**
 * ClickHouse Database Prefixing Test
 *
 * Tests the automatic database prefixing functionality for SQL statements.
 */
class ClickHouseDatabasePrefixingTest extends TestCase
{
    protected ClickHouseConnection $connection;

    protected $mockClient;

    /**
     * Set up the test environment.
     *
     * @return void
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->mockClient = Mockery::mock(Client::class);

        $config = [
            'host' => '127.0.0.1',
            'port' => 8123,
            'username' => 'default',
            'password' => '',
            'database' => 'test_db',
        ];

        $this->connection = new ClickHouseConnection($config);

        // Use reflection to inject the mock client
        $reflection = new \ReflectionClass($this->connection);
        $clientProperty = $reflection->getProperty('client');
        $clientProperty->setAccessible(true);
        $clientProperty->setValue($this->connection, $this->mockClient);
    }

    /**
     * Test OPTIMIZE TABLE statement gets database prefix.
     *
     * @return void
     */
    public function test_optimize_table_gets_database_prefix(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('OPTIMIZE TABLE test_db.test_table FINAL', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('OPTIMIZE TABLE test_table FINAL');

        $this->assertTrue($result);
    }

    /**
     * Test DROP TABLE statement gets database prefix.
     *
     * @return void
     */
    public function test_drop_table_gets_database_prefix(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('DROP TABLE test_db.test_table', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('DROP TABLE test_table');

        $this->assertTrue($result);
    }

    /**
     * Test CREATE TABLE statement gets database prefix.
     *
     * @return void
     */
    public function test_create_table_gets_database_prefix(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('CREATE TABLE test_db.new_table (id UInt64) ENGINE = MergeTree() ORDER BY id', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('CREATE TABLE new_table (id UInt64) ENGINE = MergeTree() ORDER BY id');

        $this->assertTrue($result);
    }

    /**
     * Test TRUNCATE TABLE statement gets database prefix.
     *
     * @return void
     */
    public function test_truncate_table_gets_database_prefix(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('TRUNCATE TABLE test_db.test_table', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('TRUNCATE TABLE test_table');

        $this->assertTrue($result);
    }

    /**
     * Test INSERT INTO statement gets database prefix.
     *
     * @return void
     */
    public function test_insert_into_gets_database_prefix(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('INSERT INTO test_db.test_table VALUES (1, \'test\')', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('INSERT INTO test_table VALUES (1, \'test\')');

        $this->assertTrue($result);
    }

    /**
     * Test FROM clause gets database prefix.
     *
     * @return void
     */
    public function test_from_clause_gets_database_prefix(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('SELECT * FROM test_db.test_table', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('SELECT * FROM test_table');

        $this->assertTrue($result);
    }

    /**
     * Test simple JOIN clause gets database prefix.
     *
     * @return void
     */
    public function test_simple_join_gets_database_prefix(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('SELECT * FROM test_db.users JOIN test_db.orders', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('SELECT * FROM users JOIN orders');

        $this->assertTrue($result);
    }

    /**
     * Test ALTER TABLE statement gets database prefix.
     *
     * @return void
     */
    public function test_alter_table_gets_database_prefix(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('ALTER TABLE test_db.test_table UPDATE status = \'active\' WHERE id = 1', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('ALTER TABLE test_table UPDATE status = \'active\' WHERE id = 1');

        $this->assertTrue($result);
    }

    /**
     * Test queries with existing database prefix are not modified.
     *
     * @return void
     */
    public function test_existing_database_prefix_not_modified(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('OPTIMIZE TABLE custom_db.test_table FINAL', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('OPTIMIZE TABLE custom_db.test_table FINAL');

        $this->assertTrue($result);
    }

    /**
     * Test default database connection does not add prefix.
     *
     * @return void
     */
    public function test_default_database_no_prefix(): void
    {
        // Create connection with default database
        $config = [
            'host' => '127.0.0.1',
            'port' => 8123,
            'username' => 'default',
            'password' => '',
            'database' => 'default',
        ];

        $defaultConnection = new ClickHouseConnection($config);

        // Use reflection to inject the mock client
        $reflection = new \ReflectionClass($defaultConnection);
        $clientProperty = $reflection->getProperty('client');
        $clientProperty->setAccessible(true);
        $clientProperty->setValue($defaultConnection, $this->mockClient);

        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('OPTIMIZE TABLE test_table FINAL', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $defaultConnection->statement('OPTIMIZE TABLE test_table FINAL');

        $this->assertTrue($result);
    }

    /**
     * Test case insensitive SQL keywords.
     *
     * @return void
     */
    public function test_case_insensitive_keywords(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('optimize table test_db.test_table final', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('optimize table test_table final');

        $this->assertTrue($result);
    }

    /**
     * Test mixed case SQL keywords.
     *
     * @return void
     */
    public function test_mixed_case_keywords(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('Optimize Table test_db.test_table Final', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('Optimize Table test_table Final');

        $this->assertTrue($result);
    }

    /**
     * Test table names with underscores and numbers are handled correctly.
     *
     * @return void
     */
    public function test_table_names_with_underscores_and_numbers(): void
    {
        $mockStatement = Mockery::mock(Statement::class);

        $this->mockClient->shouldReceive('write')
            ->with('OPTIMIZE TABLE test_db.user_events_v2 FINAL', [])
            ->once()
            ->andReturn($mockStatement);

        $result = $this->connection->statement('OPTIMIZE TABLE user_events_v2 FINAL');

        $this->assertTrue($result);
    }

    /**
     * Test different JOIN types get database prefix.
     *
     * @return void
     */
    public function test_different_join_types_get_prefix(): void
    {
        $testCases = [
            'INNER JOIN' => 'SELECT * FROM test_db.users INNER JOIN test_db.orders',
            'LEFT JOIN' => 'SELECT * FROM test_db.users LEFT JOIN test_db.orders',
            'RIGHT JOIN' => 'SELECT * FROM test_db.users RIGHT JOIN test_db.orders',
            'FULL JOIN' => 'SELECT * FROM test_db.users FULL JOIN test_db.orders',
        ];

        foreach ($testCases as $joinType => $expectedQuery) {
            $mockStatement = Mockery::mock(Statement::class);

            $this->mockClient->shouldReceive('write')
                ->with($expectedQuery, [])
                ->once()
                ->andReturn($mockStatement);

            $inputQuery = str_replace('test_db.', '', $expectedQuery);
            $result = $this->connection->statement($inputQuery);

            $this->assertTrue($result, "Failed for $joinType");
        }
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
