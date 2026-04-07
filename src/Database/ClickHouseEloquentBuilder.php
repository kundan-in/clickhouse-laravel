<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Eloquent\Builder;

/**
 * ClickHouse Eloquent builder for Laravel.
 *
 * Extends Laravel's Eloquent Builder with the `all()` convenience method.
 * All standard Eloquent operations (find, where, get, etc.) are handled
 * by the parent builder and work with ClickHouse via the query grammar.
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
}
