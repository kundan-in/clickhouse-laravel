<?php

namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Eloquent\Model;
use KundanIn\ClickHouseLaravel\Casts\ClickHouseArray;
use KundanIn\ClickHouseLaravel\Casts\ClickHouseJson;

/**
 * ClickHouse Eloquent Model
 *
 * This class extends Laravel's Eloquent Model to provide ClickHouse-specific
 * functionality and ensure compatibility with ClickHouse database operations.
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
     * @return \Illuminate\Database\Query\Builder
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
     * Begin querying the model with proper builder.
     *
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder
     */
    public static function query()
    {
        return (new static)->newQuery();
    }

    /**
     * Create a new Eloquent query builder for the model.
     *
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder
     */
    public function newQuery()
    {
        return $this->registerGlobalScopes($this->newQueryWithoutScopes());
    }

    /**
     * Get a new query builder that doesn't have any global scopes or eager loading.
     *
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder
     */
    public function newQueryWithoutScopes()
    {
        return $this->newEloquentBuilder($this->newBaseQueryBuilder())->setModel($this);
    }

    /**
     * Handle dynamic method calls into the model.
     * This ensures proper method resolution for ClickHouse-specific methods.
     *
     * @param  string  $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        // Handle the case where someone calls ->all() on a query builder instance
        if ($method === 'all') {
            return $this->newQuery()->get($parameters[0] ?? ['*']);
        }

        return parent::__call($method, $parameters);
    }

    /**
     * Begin querying the model on the write connection.
     *
     * @return \KundanIn\ClickHouseLaravel\Database\ClickHouseEloquentBuilder
     */
    public static function on($connection = null)
    {
        $instance = new static;
        $instance->setConnection($connection);

        return $instance->newQuery();
    }

    /**
     * Handle dynamic static method calls into the model.
     *
     * @param  string  $method
     * @param  array  $parameters
     * @return mixed
     */
    public static function __callStatic($method, $parameters)
    {
        return (new static)->$method(...$parameters);
    }

    /**
     * Create a new instance of the given model and set connection.
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
     * Define a one-to-one relationship optimized for ClickHouse.
     *
     * @param  string  $related
     * @param  string|null  $foreignKey
     * @param  string|null  $localKey
     * @return \Illuminate\Database\Eloquent\Relations\HasOne
     */
    public function hasOne($related, $foreignKey = null, $localKey = null)
    {
        $instance = $this->newRelatedInstance($related);

        $foreignKey = $foreignKey ?: $this->getForeignKey();
        $localKey = $localKey ?: $this->getKeyName();

        return $this->newHasOne($instance->newQuery(), $this, $instance->getTable().'.'.$foreignKey, $localKey);
    }

    /**
     * Define a one-to-many relationship optimized for ClickHouse.
     *
     * @param  string  $related
     * @param  string|null  $foreignKey
     * @param  string|null  $localKey
     * @return \Illuminate\Database\Eloquent\Relations\HasMany
     */
    public function hasMany($related, $foreignKey = null, $localKey = null)
    {
        $instance = $this->newRelatedInstance($related);

        $foreignKey = $foreignKey ?: $this->getForeignKey();
        $localKey = $localKey ?: $this->getKeyName();

        return $this->newHasMany($instance->newQuery(), $this, $instance->getTable().'.'.$foreignKey, $localKey);
    }

    /**
     * Define an inverse one-to-one or many relationship optimized for ClickHouse.
     *
     * @param  string  $related
     * @param  string|null  $foreignKey
     * @param  string|null  $ownerKey
     * @param  string|null  $relation
     * @return \Illuminate\Database\Eloquent\Relations\BelongsTo
     */
    public function belongsTo($related, $foreignKey = null, $ownerKey = null, $relation = null)
    {
        if (is_null($relation)) {
            $relation = $this->guessBelongsToRelation();
        }

        $instance = $this->newRelatedInstance($related);

        if (is_null($foreignKey)) {
            $foreignKey = snake_case($relation).'_'.$instance->getKeyName();
        }

        $ownerKey = $ownerKey ?: $instance->getKeyName();

        return $this->newBelongsTo($instance->newQuery(), $this, $foreignKey, $ownerKey, $relation);
    }

    /**
     * Create ClickHouse-optimized belongs to many relationship.
     * Note: This is emulated since ClickHouse doesn't have traditional pivot tables.
     *
     * @param  string  $related
     * @param  string|null  $table
     * @param  string|null  $foreignPivotKey
     * @param  string|null  $relatedPivotKey
     * @param  string|null  $parentKey
     * @param  string|null  $relatedKey
     * @param  string|null  $relation
     * @return \Illuminate\Database\Eloquent\Relations\BelongsToMany
     */
    public function belongsToMany($related, $table = null, $foreignPivotKey = null, $relatedPivotKey = null, $parentKey = null, $relatedKey = null, $relation = null)
    {
        if (is_null($relation)) {
            $relation = $this->guessBelongsToManyRelation();
        }

        $instance = $this->newRelatedInstance($related);

        $foreignPivotKey = $foreignPivotKey ?: $this->getForeignKey();
        $relatedPivotKey = $relatedPivotKey ?: $instance->getForeignKey();

        if (is_null($table)) {
            $table = $this->joiningTable($related, $instance);
        }

        return $this->newBelongsToMany(
            $instance->newQuery(), $this, $table, $foreignPivotKey,
            $relatedPivotKey, $parentKey ?: $this->getKeyName(),
            $relatedKey ?: $instance->getKeyName(), $relation
        );
    }

    /**
     * Get the default casts array for ClickHouse models.
     *
     * @return array
     */
    protected function casts(): array
    {
        return array_merge(parent::casts(), [
            // Add default ClickHouse-specific casts
        ]);
    }

    /**
     * Cast an attribute to a native ClickHouse array.
     *
     * @param  string  $key
     * @param  string  $type
     * @return $this
     */
    public function castAsArray($key, $type = 'String')
    {
        $this->casts[$key] = ClickHouseArray::class.':'.$type;

        return $this;
    }

    /**
     * Cast an attribute to JSON.
     *
     * @param  string  $key
     * @param  bool  $associative
     * @return $this
     */
    public function castAsJson($key, $associative = true)
    {
        $this->casts[$key] = ClickHouseJson::class.':'.($associative ? 'true' : 'false');

        return $this;
    }

    /**
     * Perform a model soft delete (ClickHouse doesn't support real deletes).
     * This sets a deleted flag or timestamp instead.
     *
     * @return bool
     */
    public function delete()
    {
        if (is_null($this->getKeyName())) {
            throw new \Exception('No primary key defined on model.');
        }

        if (! $this->exists) {
            return false;
        }

        if ($this->fireModelEvent('deleting') === false) {
            return false;
        }

        // Check if model uses soft deletes with flag
        if (method_exists($this, 'runSoftDelete')) {
            return $this->runSoftDelete();
        }

        // For ClickHouse, we typically mark as deleted rather than actual delete
        if (in_array('deleted_at', $this->fillable) || isset($this->casts['deleted_at'])) {
            $this->deleted_at = now();
            $this->save();
        } elseif (in_array('is_deleted', $this->fillable) || isset($this->casts['is_deleted'])) {
            $this->is_deleted = 1;
            $this->save();
        }

        $this->exists = false;

        $this->fireModelEvent('deleted', false);

        return true;
    }

    /**
     * Restore a soft-deleted model instance.
     *
     * @return bool|null
     */
    public function restore()
    {
        if ($this->fireModelEvent('restoring') === false) {
            return false;
        }

        if (in_array('deleted_at', $this->fillable) || isset($this->casts['deleted_at'])) {
            $this->deleted_at = null;
        } elseif (in_array('is_deleted', $this->fillable) || isset($this->casts['is_deleted'])) {
            $this->is_deleted = 0;
        }

        $this->exists = true;
        $result = $this->save();

        $this->fireModelEvent('restored', false);

        return $result;
    }

    /**
     * Determine if the model instance has been soft-deleted.
     *
     * @return bool
     */
    public function trashed()
    {
        if (isset($this->deleted_at)) {
            return ! is_null($this->deleted_at);
        }

        if (isset($this->is_deleted)) {
            return $this->is_deleted == 1;
        }

        return false;
    }
}
