<?php

namespace KundanIn\ClickHouseLaravel\Tests\Unit;

use KundanIn\ClickHouseLaravel\Casts\ClickHouseArray;
use KundanIn\ClickHouseLaravel\Casts\ClickHouseJson;
use KundanIn\ClickHouseLaravel\Database\ClickHouseModel;
use KundanIn\ClickHouseLaravel\Tests\TestCase;

/**
 * Test model with JSON and Array casts
 */
class TestModelWithCasts extends ClickHouseModel
{
    protected $table = 'test_casts';

    public $timestamps = false;

    protected function casts(): array
    {
        return [
            'json_data' => ClickHouseJson::class,
            'array_data' => ClickHouseArray::class,
        ];
    }
}

/**
 * Test JSON normalization and casting functionality
 */
class ClickHouseCastJsonNormalizationTest extends TestCase
{
    /**
     * Test ClickHouseJson cast handles pre-parsed arrays from ClickHouse
     */
    public function test_clickhouse_json_cast_handles_pre_parsed_arrays()
    {
        $model = new TestModelWithCasts;
        $cast = new ClickHouseJson(true); // associative = true

        // Test with pre-parsed array (what ClickHouse might return directly)
        $preparsedArray = ['name' => 'John', 'age' => 30, 'city' => 'New York'];
        $result = $cast->get($model, 'json_data', $preparsedArray, []);

        $this->assertEquals($preparsedArray, $result);
        $this->assertIsArray($result);
    }

    /**
     * Test ClickHouseJson cast handles pre-parsed objects from ClickHouse
     */
    public function test_clickhouse_json_cast_handles_pre_parsed_objects()
    {
        $model = new TestModelWithCasts;
        $cast = new ClickHouseJson(true); // associative = true

        // Test with pre-parsed object
        $preparsedObject = (object) ['name' => 'John', 'age' => 30];
        $result = $cast->get($model, 'json_data', $preparsedObject, []);

        $this->assertIsArray($result); // Should convert to array in associative mode
        $this->assertEquals(['name' => 'John', 'age' => 30], $result);
    }

    /**
     * Test ClickHouseJson cast handles JSON strings (normalized data)
     */
    public function test_clickhouse_json_cast_handles_json_strings()
    {
        $model = new TestModelWithCasts;
        $cast = new ClickHouseJson(true);

        // Test with JSON string (normalized by connection)
        $jsonString = '{"name":"John","age":30,"city":"New York"}';
        $result = $cast->get($model, 'json_data', $jsonString, []);

        $expected = ['name' => 'John', 'age' => 30, 'city' => 'New York'];
        $this->assertEquals($expected, $result);
    }

    /**
     * Test ClickHouseArray cast handles pre-parsed arrays
     */
    public function test_clickhouse_array_cast_handles_pre_parsed_arrays()
    {
        $model = new TestModelWithCasts;
        $cast = new ClickHouseArray('String');

        // Test with pre-parsed array (what ClickHouse might return)
        $preparsedArray = ['apple', 'banana', 'cherry'];
        $result = $cast->get($model, 'array_data', $preparsedArray, []);

        $this->assertEquals($preparsedArray, $result);
        $this->assertIsArray($result);
    }

    /**
     * Test ClickHouseArray cast handles JSON normalized arrays
     */
    public function test_clickhouse_array_cast_handles_json_normalized_arrays()
    {
        $model = new TestModelWithCasts;
        $cast = new ClickHouseArray('String');

        // Test with JSON string (normalized by connection)
        $jsonArray = '["apple","banana","cherry"]';
        $result = $cast->get($model, 'array_data', $jsonArray, []);

        $expected = ['apple', 'banana', 'cherry'];
        $this->assertEquals($expected, $result);
    }

    /**
     * Test ClickHouseArray cast handles ClickHouse native array format
     */
    public function test_clickhouse_array_cast_handles_native_format()
    {
        $model = new TestModelWithCasts;
        $cast = new ClickHouseArray('String');

        // Test with ClickHouse native array format
        $clickhouseFormat = "['apple','banana','cherry']";
        $result = $cast->get($model, 'array_data', $clickhouseFormat, []);

        $expected = ['apple', 'banana', 'cherry'];
        $this->assertEquals($expected, $result);
    }
}
