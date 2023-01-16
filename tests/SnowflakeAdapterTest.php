<?php

namespace Szabacsik\Phinx\Tests;

use PHPUnit\Framework\TestCase;
use Szabacsik\Phinx\SnowflakeAdapter;

class SnowflakeAdapterTest extends TestCase
{
    public function testHasTransactions()
    {
        $this->assertTrue((new SnowflakeAdapter([]))->hasTransactions());
    }
}
