<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Expression;
use InvalidArgumentException;

/**
 * ClickHouse Query Builder
 *
 * This class extends Laravel's Query Builder to provide ClickHouse-specific
 * query building capabilities and handles ClickHouse limitations.
 */
class ClickHouseQueryBuilder extends Builder
{
    /**
     * All of the available aggregate functions.
     *
     * @var array
     */
    protected $aggregates = [
        'count', 'max', 'min', 'sum', 'avg', 'any', 'anyLast', 'anyHeavy',
        'argMin', 'argMax', 'avgWeighted', 'topK', 'topKWeighted', 'groupArray',
        'groupUniqArray', 'sumMap', 'maxMap', 'minMap', 'skewPop', 'skewSamp',
        'kurtPop', 'kurtSamp', 'uniq', 'uniqExact', 'uniqCombined', 'uniqHLL12',
        'quantile', 'quantiles', 'quantileExact', 'quantileExactWeighted',
        'quantileTiming', 'quantileTimingWeighted', 'quantileDeterministic',
        'quantileTDigest', 'quantileTDigestWeighted', 'median', 'varPop', 'varSamp',
        'stddevPop', 'stddevSamp', 'covarPop', 'covarSamp', 'corr',
    ];

    /**
     * Add a "where" clause comparing two columns to the query.
     * ClickHouse has specific handling for column comparisons.
     *
     * @param  string|array  $first
     * @param  string|null  $operator
     * @param  string|null  $second
     * @param  string|null  $boolean
     * @return $this
     */
    public function whereColumn($first, $operator = null, $second = null, $boolean = 'and')
    {
        // ClickHouse supports column comparisons
        return parent::whereColumn($first, $operator, $second, $boolean);
    }

    /**
     * Add a "where in" clause to the query.
     * Optimized for ClickHouse's IN performance.
     *
     * @param  string  $column
     * @param  mixed  $values
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereIn($column, $values, $boolean = 'and', $not = false)
    {
        if (is_array($values) && count($values) > 1000) {
            // For large arrays, consider using a subquery or temp table in ClickHouse
            // For now, we'll proceed with the standard approach
        }

        return parent::whereIn($column, $values, $boolean, $not);
    }

    /**
     * Add an array "has" clause to the query (ClickHouse specific).
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
     * Add a join clause to the query.
     * ClickHouse has limited join support.
     *
     * @param  string  $table
     * @param  \Closure|string  $first
     * @param  string|null  $operator
     * @param  string|null  $second
     * @param  string  $type
     * @param  bool  $where
     * @return $this
     *
     * @throws \InvalidArgumentException
     */
    public function join($table, $first, $operator = null, $second = null, $type = 'inner', $where = false)
    {
        // ClickHouse only supports INNER and LEFT joins efficiently
        $allowedTypes = ['inner', 'left'];

        if (! in_array(strtolower($type), $allowedTypes)) {
            throw new InvalidArgumentException("ClickHouse only supports INNER and LEFT joins. {$type} join is not supported.");
        }

        return parent::join($table, $first, $operator, $second, $type, $where);
    }

    /**
     * Add a right join to the query.
     * Not supported in ClickHouse.
     *
     * @param  string  $table
     * @param  \Closure|string  $first
     * @param  string|null  $operator
     * @param  string|null  $second
     * @return $this
     *
     * @throws \InvalidArgumentException
     */
    public function rightJoin($table, $first, $operator = null, $second = null)
    {
        throw new InvalidArgumentException('RIGHT JOIN is not supported in ClickHouse.');
    }

    /**
     * Add a full outer join to the query.
     * Not supported in ClickHouse.
     *
     * @param  string  $table
     * @param  \Closure|string  $first
     * @param  string|null  $operator
     * @param  string|null  $second
     * @return $this
     *
     * @throws \InvalidArgumentException
     */
    public function fullOuterJoin($table, $first, $operator = null, $second = null)
    {
        throw new InvalidArgumentException('FULL OUTER JOIN is not supported in ClickHouse.');
    }

    /**
     * Add a "group by" clause to the query.
     * Enhanced for ClickHouse WITH ROLLUP and CUBE.
     *
     * @param  array|string  ...$groups
     * @return $this
     */
    public function groupBy(...$groups)
    {
        foreach ($groups as $group) {
            if (is_string($group) && str_contains($group, 'WITH ')) {
                // Handle ClickHouse-specific GROUP BY WITH ROLLUP/CUBE
                $this->groups[] = new Expression($group);
            } else {
                parent::groupBy($group);
            }
        }

        return $this;
    }

    /**
     * Add a "group by with rollup" clause to the query (ClickHouse specific).
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
     * Add a "group by with cube" clause to the query (ClickHouse specific).
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
     * Add a "sample" clause to the query (ClickHouse specific).
     *
     * @param  float  $ratio
     * @return $this
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
     * Add a "final" clause to the query (ClickHouse specific for ReplacingMergeTree).
     *
     * @return $this
     */
    public function final()
    {
        $this->final = true;

        return $this;
    }

    /**
     * Add a "prewhere" clause to the query (ClickHouse optimization).
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
            $this->addBinding($value, 'prewhere');
        }

        return $this;
    }

    /**
     * Execute an aggregate function on the database.
     * Enhanced with ClickHouse-specific aggregates.
     *
     * @param  string  $function
     * @param  array  $columns
     * @return mixed
     */
    public function aggregate($function, $columns = ['*'])
    {
        $results = $this->cloneWithout($this->unions ? [] : ['columns'])
            ->cloneWithoutBindings($this->unions ? [] : ['select'])
            ->setAggregate($function, $columns)
            ->get($columns);

        if (! $results->isEmpty()) {
            return array_change_key_case((array) $results[0])['aggregate'];
        }

        return null;
    }

    /**
     * Retrieve the "count" result of the query with ClickHouse optimizations.
     *
     * @param  string  $columns
     * @return int
     */
    public function count($columns = '*')
    {
        // ClickHouse count() is very fast
        return (int) $this->aggregate(__FUNCTION__, is_array($columns) ? $columns : [$columns]);
    }

    /**
     * Add a ClickHouse-specific uniq count.
     *
     * @param  string  $column
     * @return mixed
     */
    public function uniq($column = '*')
    {
        return $this->aggregate('uniq', [$column]);
    }

    /**
     * Add a ClickHouse-specific uniqExact count.
     *
     * @param  string  $column
     * @return mixed
     */
    public function uniqExact($column = '*')
    {
        return $this->aggregate('uniqExact', [$column]);
    }

    /**
     * Get the SQL representation of the query.
     *
     * @return string
     */
    public function toSql()
    {
        return $this->grammar->compileSelect($this);
    }

    /**
     * Clone the query without the given properties.
     *
     * @param  array  $properties
     * @return static
     */
    public function cloneWithout(array $properties)
    {
        return tap(clone $this, function ($clone) use ($properties) {
            foreach ($properties as $property) {
                $clone->{$property} = null;
            }
        });
    }

    /**
     * Clone the query without the given bindings.
     *
     * @param  array  $except
     * @return static
     */
    public function cloneWithoutBindings(array $except)
    {
        return tap(clone $this, function ($clone) use ($except) {
            foreach ($except as $type) {
                $clone->bindings[$type] = [];
            }
        });
    }

    /**
     * Add a "where date" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @param  string  $boolean
     * @return $this
     */
    public function whereDate($column, $operator = null, $value = null, $boolean = 'and')
    {
        [$value, $operator] = $this->prepareValueAndOperator($value, $operator, func_num_args() === 2);

        $type = 'Date';

        $this->wheres[] = compact('type', 'column', 'operator', 'value', 'boolean');

        $this->addBinding($value, 'where');

        return $this;
    }

    /**
     * Add a "where time" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @param  string  $boolean
     * @return $this
     */
    public function whereTime($column, $operator = null, $value = null, $boolean = 'and')
    {
        [$value, $operator] = $this->prepareValueAndOperator($value, $operator, func_num_args() === 2);

        $type = 'Time';

        $this->wheres[] = compact('type', 'column', 'operator', 'value', 'boolean');

        $this->addBinding($value, 'where');

        return $this;
    }

    /**
     * Add a "where day" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @param  string  $boolean
     * @return $this
     */
    public function whereDay($column, $operator = null, $value = null, $boolean = 'and')
    {
        [$value, $operator] = $this->prepareValueAndOperator($value, $operator, func_num_args() === 2);

        $type = 'Day';

        $this->wheres[] = compact('type', 'column', 'operator', 'value', 'boolean');

        $this->addBinding($value, 'where');

        return $this;
    }

    /**
     * Add a "where month" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @param  string  $boolean
     * @return $this
     */
    public function whereMonth($column, $operator = null, $value = null, $boolean = 'and')
    {
        [$value, $operator] = $this->prepareValueAndOperator($value, $operator, func_num_args() === 2);

        $type = 'Month';

        $this->wheres[] = compact('type', 'column', 'operator', 'value', 'boolean');

        $this->addBinding($value, 'where');

        return $this;
    }

    /**
     * Add a "where year" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @param  string  $boolean
     * @return $this
     */
    public function whereYear($column, $operator = null, $value = null, $boolean = 'and')
    {
        [$value, $operator] = $this->prepareValueAndOperator($value, $operator, func_num_args() === 2);

        $type = 'Year';

        $this->wheres[] = compact('type', 'column', 'operator', 'value', 'boolean');

        $this->addBinding($value, 'where');

        return $this;
    }

    /**
     * Add a "where between" clause to the query.
     *
     * @param  string  $column
     * @param  \Traversable|array  $values
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereBetween($column, \Traversable|array $values, $boolean = 'and', $not = false)
    {
        $type = 'Between';

        if ($not) {
            $type = 'NotBetween';
        }

        // Convert Traversable to array
        if ($values instanceof \Traversable) {
            $values = iterator_to_array($values);
        }

        if (count($values) !== 2) {
            throw new InvalidArgumentException('whereBetween expects exactly 2 values.');
        }

        $this->wheres[] = compact('type', 'column', 'values', 'boolean');

        $this->addBinding($values, 'where');

        return $this;
    }

    /**
     * Add a "where not between" clause to the query.
     *
     * @param  string  $column
     * @param  \Traversable|array  $values
     * @param  string  $boolean
     * @return $this
     */
    public function whereNotBetween($column, \Traversable|array $values, $boolean = 'and')
    {
        return $this->whereBetween($column, $values, $boolean, true);
    }

    /**
     * Add a "where between columns" clause to the query.
     *
     * @param  string  $column
     * @param  \Traversable|array  $values
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereBetweenColumns($column, \Traversable|array $values, $boolean = 'and', $not = false)
    {
        $type = 'BetweenColumns';

        if ($not) {
            $type = 'NotBetweenColumns';
        }

        // Convert Traversable to array
        if ($values instanceof \Traversable) {
            $values = iterator_to_array($values);
        }

        if (count($values) !== 2) {
            throw new InvalidArgumentException('whereBetweenColumns expects exactly 2 values.');
        }

        $this->wheres[] = compact('type', 'column', 'values', 'boolean');

        return $this;
    }

    /**
     * Add a "where not between columns" clause to the query.
     *
     * @param  string  $column
     * @param  \Traversable|array  $values
     * @param  string  $boolean
     * @return $this
     */
    public function whereNotBetweenColumns($column, \Traversable|array $values, $boolean = 'and')
    {
        return $this->whereBetweenColumns($column, $values, $boolean, true);
    }

    /**
     * Add a "where null" clause to the query.
     *
     * @param  string|array  $columns
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereNull($columns, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotNull' : 'Null';

        if (is_array($columns)) {
            foreach ($columns as $column) {
                $this->wheres[] = compact('type', 'column', 'boolean');
            }
        } else {
            $column = $columns;
            $this->wheres[] = compact('type', 'column', 'boolean');
        }

        return $this;
    }

    /**
     * Add a "where not null" clause to the query.
     *
     * @param  string|array  $columns
     * @param  string  $boolean
     * @return $this
     */
    public function whereNotNull($columns, $boolean = 'and')
    {
        return $this->whereNull($columns, $boolean, true);
    }

    /**
     * Add a "where like" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $value
     * @param  bool  $caseSensitive
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereLike($column, $value, $caseSensitive = false, $boolean = 'and', $not = false)
    {
        $operator = $not ? 'not like' : 'like';

        return $this->where($column, $operator, $value, $boolean);
    }

    /**
     * Add a "where not like" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $value
     * @param  bool  $caseSensitive
     * @param  string  $boolean
     * @return $this
     */
    public function whereNotLike($column, $value, $caseSensitive = false, $boolean = 'and')
    {
        return $this->whereLike($column, $value, $caseSensitive, $boolean, true);
    }

    /**
     * Add a "where ilike" clause to the query (case-insensitive like).
     *
     * @param  string  $column
     * @param  mixed  $value
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereILike($column, $value, $boolean = 'and', $not = false)
    {
        $operator = $not ? 'not ilike' : 'ilike';

        return $this->where($column, $operator, $value, $boolean);
    }

    /**
     * Add a "where not ilike" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $value
     * @param  string  $boolean
     * @return $this
     */
    public function whereNotILike($column, $value, $boolean = 'and')
    {
        return $this->whereILike($column, $value, $boolean, true);
    }

    /**
     * Prepare the value and operator for a where clause.
     *
     * @param  string  $value
     * @param  string  $operator
     * @param  bool  $useDefault
     * @return array
     */
    public function prepareValueAndOperator($value, $operator, $useDefault = false)
    {
        if ($useDefault) {
            return [$operator, '='];
        } elseif ($this->invalidOperatorAndValue($operator, $value)) {
            throw new InvalidArgumentException('Illegal operator and value combination.');
        }

        return [$value, $operator];
    }
}
