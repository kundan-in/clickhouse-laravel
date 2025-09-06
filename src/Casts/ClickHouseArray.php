<?php

namespace KundanIn\ClickHouseLaravel\Casts;

use Illuminate\Contracts\Database\Eloquent\CastsAttributes;
use Illuminate\Database\Eloquent\Model;
use InvalidArgumentException;

/**
 * ClickHouse Array Cast
 *
 * This cast handles ClickHouse native Array types.
 */
class ClickHouseArray implements CastsAttributes
{
    /**
     * The array element type.
     *
     * @var string
     */
    protected $elementType;

    /**
     * Create a new ClickHouse array cast instance.
     *
     * @param  string  $elementType
     */
    public function __construct($elementType = 'String')
    {
        $this->elementType = $elementType;
    }

    /**
     * Cast the given value.
     *
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @param  string  $key
     * @param  mixed  $value
     * @param  array  $attributes
     * @return array|null
     */
    public function get(Model $model, string $key, $value, array $attributes)
    {
        if (is_null($value)) {
            return null;
        }

        // If it's already an array (from ClickHouse or normalized JSON), cast elements and return
        if (is_array($value)) {
            return $this->castArrayElements($value);
        }

        if (is_string($value)) {
            // Handle empty string
            if ($value === '' || $value === '[]') {
                return [];
            }

            // Try JSON decode first (for normalized data), then ClickHouse array parsing
            if ($this->isJsonString($value)) {
                try {
                    $decoded = json_decode($value, true, 512, JSON_THROW_ON_ERROR);
                    if (is_array($decoded)) {
                        return $this->castArrayElements($decoded);
                    }
                } catch (\JsonException $e) {
                    // Fall through to ClickHouse array parsing
                }
            }

            // Parse ClickHouse array format: ['item1','item2','item3']
            $value = $this->parseClickHouseArray($value);
        }

        if (! is_array($value)) {
            return [];
        }

        return $this->castArrayElements($value);
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

        if (! is_array($value)) {
            throw new InvalidArgumentException('The value must be an array.');
        }

        return $this->formatForClickHouse($value);
    }

    /**
     * Parse ClickHouse array string format.
     *
     * @param  string  $value
     * @return array
     */
    protected function parseClickHouseArray($value)
    {
        // Handle empty array
        if ($value === '[]') {
            return [];
        }

        // Remove outer brackets
        $value = trim($value, '[]');

        if (empty($value)) {
            return [];
        }

        // Split by comma, considering quoted strings
        $elements = [];
        $current = '';
        $inQuotes = false;
        $escaping = false;

        for ($i = 0; $i < strlen($value); $i++) {
            $char = $value[$i];

            if ($escaping) {
                $current .= $char;
                $escaping = false;

                continue;
            }

            if ($char === '\\') {
                $escaping = true;

                continue;
            }

            if ($char === "'" && ! $inQuotes) {
                $inQuotes = true;

                continue;
            }

            if ($char === "'" && $inQuotes) {
                $inQuotes = false;

                continue;
            }

            if ($char === ',' && ! $inQuotes) {
                $elements[] = $this->castElement(trim($current));
                $current = '';

                continue;
            }

            $current .= $char;
        }

        if (! empty($current)) {
            $elements[] = $this->castElement(trim($current));
        }

        return $elements;
    }

    /**
     * Cast array elements to the appropriate type.
     *
     * @param  array  $array
     * @return array
     */
    protected function castArrayElements(array $array)
    {
        return array_map([$this, 'castElement'], $array);
    }

    /**
     * Cast a single element to the appropriate type.
     *
     * @param  mixed  $element
     * @return mixed
     */
    protected function castElement($element)
    {
        switch (strtolower($this->elementType)) {
            case 'int8':
            case 'int16':
            case 'int32':
            case 'int64':
            case 'uint8':
            case 'uint16':
            case 'uint32':
            case 'uint64':
                return (int) $element;

            case 'float32':
            case 'float64':
                return (float) $element;

            case 'string':
            case 'fixedstring':
                return (string) $element;

            case 'date':
            case 'datetime':
            case 'datetime64':
                return $element; // Keep as string for now, could be enhanced

            default:
                return $element;
        }
    }

    /**
     * Format array for ClickHouse storage.
     *
     * @param  array  $array
     * @return string
     */
    protected function formatForClickHouse(array $array)
    {
        if (empty($array)) {
            return '[]';
        }

        $formatted = array_map(function ($item) {
            if (is_string($item)) {
                return "'".str_replace("'", "\\'", $item)."'";
            }

            if (is_bool($item)) {
                return $item ? '1' : '0';
            }

            if (is_null($item)) {
                return 'NULL';
            }

            return (string) $item;
        }, $array);

        return '['.implode(',', $formatted).']';
    }

    /**
     * Check if a string is valid JSON.
     *
     * @param  string  $string
     * @return bool
     */
    protected function isJsonString($string): bool
    {
        return is_string($string) &&
               strlen($string) > 1 &&
               ($string[0] === '[' || $string[0] === '{') &&
               json_validate($string);
    }
}
