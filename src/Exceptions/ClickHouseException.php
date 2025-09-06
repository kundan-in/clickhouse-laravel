<?php

namespace KundanIn\ClickHouseLaravel\Exceptions;

use Exception;

/**
 * Base ClickHouse Exception
 *
 * This is the base exception class for all ClickHouse-specific exceptions.
 */
class ClickHouseException extends Exception
{
    /**
     * The ClickHouse error code.
     *
     * @var int|null
     */
    protected $clickhouseCode;

    /**
     * Create a new ClickHouse exception instance.
     *
     * @param  string  $message
     * @param  int  $code
     * @param  int|null  $clickhouseCode
     * @param  \Throwable|null  $previous
     */
    public function __construct($message = '', $code = 0, $clickhouseCode = null, ?\Throwable $previous = null)
    {
        parent::__construct($message, $code, $previous);

        $this->clickhouseCode = $clickhouseCode;
    }

    /**
     * Get the ClickHouse-specific error code.
     *
     * @return int|null
     */
    public function getClickHouseCode()
    {
        return $this->clickhouseCode;
    }

    /**
     * Create a ClickHouse exception from a generic exception.
     *
     * @param  \Throwable  $exception
     * @param  string|null  $message
     * @return static
     */
    public static function fromException(\Throwable $exception, $message = null)
    {
        return new static(
            $message ?: $exception->getMessage(),
            $exception->getCode(),
            null,
            $exception
        );
    }
}
