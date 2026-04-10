<?php


namespace KundanIn\ClickHouseLaravel\Database;

use Illuminate\Database\Query\Processors\Processor;

class ClickHouseQueryProcessor extends Processor
{
    public function processColumns($results)
    {
        return array_map(function ($result) {
            $result = (object) $result;

            $type = $result->type;

            $is_nullable = false;
            if(strncmp($type, 'Nullable(', 9) == 0) {
                $is_nullable = true;
                $type = substr($type, 9, -1);
            }

            if(strncmp($type, 'Decimal', 7) == 0) {
                $type_name = 'numeric';
            }else{
                // TODO see system.data_type_families
                $type_name = match ($type) {
                    'Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256',
                    'UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt128', 'UInt256'
                    => 'int',
                    'Float32', 'Float64', 'BFloat16' => 'float',
                    'Bool' => 'bool',
                    default => 'string',
                };
            }

            return [
                'name' => $result->name,
                'type_name' => $type_name,
                'type' => $result->type,
                'nullable' => $is_nullable,
                'default' => $result->default,
                'auto_increment' =>  false, // TODO see $result->default for generateUUIDv4 / generateSerialID
                'comment' => $result->comment,
            ];
        }, $results);
    }
}
