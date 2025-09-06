<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Eloquent\Builder;

/**
 * ClickHouse Eloquent Builder
 *
 * This class extends Laravel's Eloquent Builder to provide ClickHouse-specific
 * functionality and handle ClickHouse limitations in Eloquent queries.
 */
class ClickHouseEloquentBuilder extends Builder
{
    /**
     * Get all models from the database.
     *
     * @param  array|string  $columns
     * @return \Illuminate\Database\Eloquent\Collection
     */
    public function all($columns = ['*'])
    {
        return $this->get($columns);
    }

    /**
     * Find a model by its primary key.
     *
     * @param  mixed  $id
     * @param  array|string  $columns
     * @return \Illuminate\Database\Eloquent\Model|null
     */
    public function find($id, $columns = ['*'])
    {
        if (is_array($id) || $id instanceof \Traversable) {
            return $this->findMany($id, $columns);
        }

        return $this->whereKey($id)->first($columns);
    }

    /**
     * Find multiple models by their primary keys.
     *
     * @param  \Traversable|array  $ids
     * @param  array|string  $columns
     * @return \Illuminate\Database\Eloquent\Collection
     */
    public function findMany($ids, $columns = ['*'])
    {
        $ids = $ids instanceof \Traversable ? collect($ids)->all() : $ids;

        if (empty($ids)) {
            return $this->model->newCollection();
        }

        return $this->whereKey($ids)->get($columns);
    }

    /**
     * Find a model by its primary key or throw an exception.
     *
     * @param  mixed  $id
     * @param  array|string  $columns
     * @return \Illuminate\Database\Eloquent\Model
     *
     * @throws \Illuminate\Database\Eloquent\ModelNotFoundException
     */
    public function findOrFail($id, $columns = ['*'])
    {
        $result = $this->find($id, $columns);

        $id = $id instanceof \Traversable ? collect($id)->all() : $id;

        if (is_array($id)) {
            if (count($result) !== count(array_unique($id))) {
                throw (new \Illuminate\Database\Eloquent\ModelNotFoundException)->setModel(
                    get_class($this->model), $id
                );
            }

            return $result;
        }

        if (is_null($result)) {
            throw (new \Illuminate\Database\Eloquent\ModelNotFoundException)->setModel(
                get_class($this->model), $id
            );
        }

        return $result;
    }

    /**
     * Add a basic where clause to the query.
     *
     * @param  \Closure|string|array|\Illuminate\Database\Query\Expression  $column
     * @param  mixed  $operator
     * @param  mixed  $value
     * @param  string  $boolean
     * @return $this
     */
    public function where($column, $operator = null, $value = null, $boolean = 'and')
    {
        // Handle ClickHouse-specific where clause optimizations if needed
        return parent::where($column, $operator, $value, $boolean);
    }

    /**
     * Add a "where in" clause to the query.
     *
     * @param  string  $column
     * @param  mixed  $values
     * @param  string  $boolean
     * @param  bool  $not
     * @return $this
     */
    public function whereIn($column, $values, $boolean = 'and', $not = false)
    {
        // Handle ClickHouse-specific optimizations for IN clauses if needed
        return parent::whereIn($column, $values, $boolean, $not);
    }
}
