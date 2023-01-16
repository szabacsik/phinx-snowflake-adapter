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

    public function testBeginTransaction()
    {
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['execute']);
        $mock->expects($this->once())->method('execute')->with('begin transaction');
        $mock->beginTransaction();
    }

    public function testCommitTransaction()
    {
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['execute']);
        $mock->expects($this->once())->method('execute')->with('commit');
        $mock->commitTransaction();
    }

    public function testRollbackTransaction()
    {
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['execute']);
        $mock->expects($this->once())->method('execute')->with('rollback');
        $mock->rollbackTransaction();
    }

    public function testQuoteTableName()
    {
        $tableName = 'lorem_IPSUM';
        $expected = "\"$tableName\"";
        $this->assertEquals($expected, (new SnowflakeAdapter([]))->quoteTableName($tableName));
    }

    public function testQuoteColumnName()
    {
        $columnName = 'lorem_IPSUM';
        $expected = "\"$columnName\"";
        $this->assertEquals($expected, (new SnowflakeAdapter([]))->quoteColumnName($columnName));
    }


}
