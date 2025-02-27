<?php

namespace Inprohub\DataTables\Tests\Models;

use MongoDB\Laravel\Eloquent\Model as Eloquent;
use Inprohub\DataTables\Traits\MongodbDataTableTrait;

class Role extends Eloquent
{
    use MongodbDataTableTrait;

    protected $connection = 'mongodb';

    static protected $unguarded = true;

    public function user()
    {
        return $this->belongsTo('User');
    }
}