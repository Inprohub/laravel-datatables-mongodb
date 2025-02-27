<?php

namespace Inprohub\DataTables\Tests\Models;

use MongoDB\Laravel\Eloquent\Model as Eloquent;

class Heart extends Eloquent
{
    protected $connection = 'mongodb';

    static protected $unguarded = true;

    public function user()
    {
        return $this->hasOne(User::class);
    }
}