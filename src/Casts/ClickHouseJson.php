<?php

namespace KundanIn\ClickHouseLaravel\Casts;

use Illuminate\Contracts\Database\Eloquent\CastsAttributes;
use Illuminate\Database\Eloquent\Model;
use InvalidArgumentException;
use JsonException;

/**
 * ClickHouse JSON Cast
 *
 * This cast handles JSON data in ClickHouse, which can be stored as String or Object type.
 */
class ClickHouseJson implements CastsAttributes
{
    /**
     * Whether to return associative arrays.
     *
     * @var bool
     */
    protected $associative;

    /**
     * Create a new ClickHouse JSON cast instance.
     *
     * @param  bool  $associative
     */
    public function __construct($associative = true)
    {
        $this->associative = $associative;
    }

    /**
     * Cast the given value.
     *
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @param  string  $key
     * @param  mixed  $value
     * @param  array  $attributes
     * @return mixed
     */
    public function get(Model $model, string $key, $value, array $attributes)
    {
        if (is_null($value)) {
            return null;
        }

        // If it's already an array or object (from ClickHouse or pre-parsed), return it
        if (is_array($value)) {
            return $value;
        }

        if (is_object($value)) {
            // Convert to array if associative mode is enabled
            return $this->associative ? json_decode(json_encode($value), true) : $value;
        }

        if (! is_string($value)) {
            return $value;
        }

        // Handle empty string
        if ($value === '') {
            return $this->associative ? [] : null;
        }

        try {
            $decoded = json_decode($value, $this->associative, 512, JSON_THROW_ON_ERROR);

            return $decoded;
        } catch (JsonException $e) {
            // If JSON decoding fails, return the original value
            return $value;
        }
    }

    /**
     * Prepare the given value for storage.
     *
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @param  string  $key
     * @param  mixed  $value
     * @param  array  $attributes
     * @return string|null
     */
    public function set(Model $model, string $key, $value, array $attributes)
    {
        if (is_null($value)) {
            return null;
        }

        if (is_string($value)) {
            // Validate that it's valid JSON
            try {
                json_decode($value, true, 512, JSON_THROW_ON_ERROR);

                return $value;
            } catch (JsonException $e) {
                throw new InvalidArgumentException("Invalid JSON provided for {$key}: {$e->getMessage()}");
            }
        }

        try {
            return json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
        } catch (JsonException $e) {
            throw new InvalidArgumentException("Unable to encode value as JSON for {$key}: {$e->getMessage()}");
        }
    }
}
