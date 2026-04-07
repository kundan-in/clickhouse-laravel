<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Eloquent\Model;
use KundanIn\ClickHouseLaravel\Casts\ClickHouseArray;
use KundanIn\ClickHouseLaravel\Casts\ClickHouseJson;

/**
 * Base Eloquent model for ClickHouse tables.
 *
 * Extends Laravel's Eloquent Model and wires up the ClickHouse connection,
 * query builder, and Eloquent builder. All standard Eloquent operations
 * (create, update, delete, find, relationships, soft deletes) work as
 * expected — the underlying Connection and Grammar handle ClickHouse
 * syntax differences transparently.
 *
 * For soft deletes, use Laravel's `SoftDeletes` trait as you normally would.
 */
class ClickHouseModel extends Model
{
    /**
     * The connection name for the model.
     *
     * @var string|null
     */
    protected $connection = 'clickhouse';

    /**
     * Indicates if the model should be timestamped.
     *
     * @var bool
     */
    public $timestamps = true;

    /**
     * Create a new Eloquent query builder for the model.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder
     */
    public function newEloquentBuilder($query)
    {
        return new ClickHouseEloquentBuilder($query);
    }

    /**
     * Get a new query builder instance for the connection.
     *
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseQueryBuilder
     */
    protected function newBaseQueryBuilder()
    {
        return $this->getConnection()->query();
    }

    /**
     * Get all of the models from the database.
     *
     * @param  array|mixed  $columns
     * @return \Illuminate\Database\Eloquent\Collection|static[]
     */
    public static function all($columns = ['*'])
    {
        return static::query()->get(
            is_array($columns) ? $columns : func_get_args()
        );
    }

    /**
     * Begin querying the model.
     *
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder
     */
    public static function query()
    {
        return (new static)->newQuery();
    }

    /**
     * Get a new query builder for the model.
     *
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder
     */
    public function newQuery()
    {
        return $this->registerGlobalScopes($this->newQueryWithoutScopes());
    }

    /**
     * Get a new query builder without global scopes.
     *
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder
     */
    public function newQueryWithoutScopes()
    {
        return $this->newEloquentBuilder($this->newBaseQueryBuilder())->setModel($this);
    }

    /**
     * Create a new model instance and set the connection.
     *
     * @param  array  $attributes
     * @param  bool  $exists
     * @return static
     */
    public function newInstance($attributes = [], $exists = false)
    {
        $model = new static((array) $attributes);
        $model->exists = $exists;
        $model->setConnection($this->getConnectionName());
        $model->setTable($this->getTable());

        return $model;
    }

    /**
     * Register a cast for a ClickHouse Array column.
     *
     * @param  string  $key
     * @param  string  $type  The element type (e.g., 'String', 'UInt32').
     * @return $this
     */
    public function castAsArray($key, $type = 'String')
    {
        $this->casts[$key] = ClickHouseArray::class.':'.$type;

        return $this;
    }

    /**
     * Register a cast for a ClickHouse JSON column.
     *
     * @param  string  $key
     * @param  bool  $associative  Whether to decode as associative array.
     * @return $this
     */
    public function castAsJson($key, $associative = true)
    {
        $this->casts[$key] = ClickHouseJson::class.':'.($associative ? 'true' : 'false');

        return $this;
    }
}
