<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Expression;
use InvalidArgumentException;

/**
 * ClickHouse query builder for Laravel.
 *
 * Extends Laravel's Query Builder with ClickHouse-specific features such as
 * SAMPLE, FINAL, PREWHERE, array operations, and advanced grouping while
 * delegating standard SQL operations to the parent builder.
 */
class ClickHouseQueryBuilder extends Builder
{
    /**
     * The sample ratio for SAMPLE clause.
     *
     * @var float|null
     */
    public $sample = null;

    /**
     * Whether to add FINAL keyword for ReplacingMergeTree.
     *
     * @var bool
     */
    public $final = false;

    /**
     * The PREWHERE conditions for the query.
     *
     * @var array
     */
    public $prewhere = [];

    /**
     * Add a SAMPLE clause to the query.
     *
     * Instructs ClickHouse to return an approximate result based on a
     * random sample of the data. Only works with MergeTree family tables
     * that have a SAMPLE BY clause defined.
     *
     * @param  float  $ratio  A value between 0 (exclusive) and 1 (inclusive).
     * @return $this
     *
     * @throws \InvalidArgumentException
     */
    public function sample(float $ratio)
    {
        if ($ratio <= 0 || $ratio > 1) {
            throw new InvalidArgumentException('Sample ratio must be between 0 and 1.');
        }

        $this->sample = $ratio;

        return $this;
    }

    /**
     * Add the FINAL keyword to the query.
     *
     * Forces ClickHouse to merge data parts before returning results,
     * which is required for accurate reads on ReplacingMergeTree,
     * CollapsingMergeTree, and VersionedCollapsingMergeTree engines.
     *
     * @return $this
     */
    public function final()
    {
        $this->final = true;

        return $this;
    }

    /**
     * Add a PREWHERE condition to the query.
     *
     * PREWHERE is a ClickHouse optimization that filters data before
     * reading full column data, reducing I/O for selective queries.
     *
     * @param  \Closure|string|array|\Illuminate\Database\Query\Expression  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @param  string  $boolean
     * @return $this
     */
    public function prewhere($column, $operator = null, $value = null, $boolean = 'and')
    {
        if ($column instanceof \Closure) {
            return $this->whereNested($column, $boolean);
        }

        $this->prewhere[] = compact('column', 'operator', 'value', 'boolean');

        if (! $value instanceof Expression) {
            $this->addBinding($value, 'where');
        }

        return $this;
    }

    /**
     * Add a "where array has" clause to the query.
     *
     * Compiles to ClickHouse's `has(column, value)` function.
     *
     * @param  string  $column
     * @param  mixed  $value
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereArrayHas($column, $value, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotArrayHas' : 'ArrayHas';

        $this->wheres[] = compact('type', 'column', 'value', 'boolean');

        $this->addBinding($value, 'where');

        return $this;
    }

    /**
     * Add a "where array has any" clause to the query.
     *
     * Compiles to ClickHouse's `hasAny(column, [values])` function.
     *
     * @param  string  $column
     * @param  array  $values
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereArrayHasAny($column, array $values, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotArrayHasAny' : 'ArrayHasAny';

        $this->wheres[] = compact('type', 'column', 'values', 'boolean');

        foreach ($values as $value) {
            $this->addBinding($value, 'where');
        }

        return $this;
    }

    /**
     * Add a "where array has all" clause to the query.
     *
     * Compiles to ClickHouse's `hasAll(column, [values])` function.
     *
     * @param  string  $column
     * @param  array  $values
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereArrayHasAll($column, array $values, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotArrayHasAll' : 'ArrayHasAll';

        $this->wheres[] = compact('type', 'column', 'values', 'boolean');

        foreach ($values as $value) {
            $this->addBinding($value, 'where');
        }

        return $this;
    }

    /**
     * Add a GROUP BY WITH ROLLUP clause to the query.
     *
     * @param  array|string  ...$groups
     * @return $this
     */
    public function groupByWithRollup(...$groups)
    {
        $groupList = implode(', ', array_map([$this->grammar, 'wrap'], $groups));

        $this->groups[] = new Expression("{$groupList} WITH ROLLUP");

        return $this;
    }

    /**
     * Add a GROUP BY WITH CUBE clause to the query.
     *
     * @param  array|string  ...$groups
     * @return $this
     */
    public function groupByWithCube(...$groups)
    {
        $groupList = implode(', ', array_map([$this->grammar, 'wrap'], $groups));

        $this->groups[] = new Expression("{$groupList} WITH CUBE");

        return $this;
    }

    /**
     * Get the approximate unique count using ClickHouse's uniq() function.
     *
     * @param  string  $column
     * @return mixed
     */
    public function uniq($column = '*')
    {
        return $this->aggregate('uniq', [$column]);
    }

    /**
     * Get the exact unique count using ClickHouse's uniqExact() function.
     *
     * @param  string  $column
     * @return mixed
     */
    public function uniqExact($column = '*')
    {
        return $this->aggregate('uniqExact', [$column]);
    }

    /**
     * Insert a new record and get the value of the primary key.
     *
     * ClickHouse does not support auto-incrementing primary keys.
     * Use UUIDs or application-generated IDs instead.
     *
     * @param  array  $values
     * @param  string|null  $sequence
     * @return never
     *
     * @throws \KundanIn\ClickHouseLaravel\Exceptions\UnsupportedOperationException
     */
    public function insertGetId(array $values, $sequence = null)
    {
        throw \KundanIn\ClickHouseLaravel\Exceptions\UnsupportedOperationException::forOperation(
            'insertGetId',
            'ClickHouse does not support auto-incrementing IDs. Use UUID or supply your own identifier.'
        );
    }

    /**
     * Insert new records or update existing ones (upsert).
     *
     * ClickHouse does not support traditional UPSERT. Use ReplacingMergeTree
     * engine with FINAL keyword for deduplication instead.
     *
     * @param  array  $values
     * @param  array|string  $uniqueBy
     * @param  array|null  $update
     * @return int
     *
     * @throws \KundanIn\ClickHouseLaravel\Exceptions\UnsupportedOperationException
     */
    public function upsert(array $values, $uniqueBy, $update = null)
    {
        throw \KundanIn\ClickHouseLaravel\Exceptions\UnsupportedOperationException::forOperation(
            'upsert',
            'ClickHouse does not support UPSERT. Use ReplacingMergeTree engine with the FINAL keyword for deduplication.'
        );
    }
}
