<?php
namespace Inprohub\DataTables\Traits;

use Inprohub\DataTables\MongodbDataTable;

trait MongodbDataTableTrait
{
    /**
     * Get Mongodb DataTable instance for a model.
     *
     * @return MongodbDataTable
     */
    public static function dataTable()
    {
        return new MongodbDataTable(new static);
    }
}
