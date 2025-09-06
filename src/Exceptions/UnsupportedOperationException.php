<?php

namespace KundanIn\ClickHouseLaravel\Exceptions;

/**
 * Unsupported Operation Exception
 *
 * Thrown when attempting to perform an operation that is not supported by ClickHouse.
 */
class UnsupportedOperationException extends ClickHouseException
{
    /**
     * Create a new unsupported operation exception.
     *
     * @param  string  $operation
     * @param  string|null  $reason
     * @return static
     */
    public static function forOperation($operation, $reason = null)
    {
        $message = "The operation '{$operation}' is not supported by ClickHouse.";

        if ($reason) {
            $message .= " Reason: {$reason}";
        }

        return new static($message);
    }

    /**
     * Create exception for unsupported joins.
     *
     * @param  string  $joinType
     * @return static
     */
    public static function forJoin($joinType)
    {
        return static::forOperation(
            "{$joinType} JOIN",
            'ClickHouse only supports INNER and LEFT joins efficiently.'
        );
    }

    /**
     * Create exception for unsupported transactions.
     *
     * @return static
     */
    public static function forTransactions()
    {
        return static::forOperation(
            'transactions',
            'ClickHouse is an OLAP database and does not support traditional ACID transactions.'
        );
    }

    /**
     * Create exception for unsupported foreign keys.
     *
     * @return static
     */
    public static function forForeignKeys()
    {
        return static::forOperation(
            'foreign keys',
            'ClickHouse does not support foreign key constraints.'
        );
    }

    /**
     * Create exception for unsupported updates without WHERE.
     *
     * @return static
     */
    public static function forUpdateWithoutWhere()
    {
        return static::forOperation(
            'UPDATE without WHERE clause',
            'ClickHouse requires WHERE clause for UPDATE operations for safety.'
        );
    }

    /**
     * Create exception for unsupported deletes without WHERE.
     *
     * @return static
     */
    public static function forDeleteWithoutWhere()
    {
        return static::forOperation(
            'DELETE without WHERE clause',
            'ClickHouse requires WHERE clause for DELETE operations for safety.'
        );
    }
}
