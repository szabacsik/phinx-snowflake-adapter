<?php

namespace Szabacsik\Phinx\Tests;

use Phinx\Db\Table\Column;
use Phinx\Db\Table\Table;
use PHPUnit\Framework\TestCase;
use Szabacsik\Phinx\SnowflakeAdapter;
use Phinx\Config\Config;

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

    public function testHasTable()
    {
        $tableName = 'lorem_IPSUM';
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['fetchRow']);
        $mock->expects($this->exactly(2))
            ->method('fetchRow')
            ->with("show tables like '$tableName'")
            ->willReturnOnConsecutiveCalls([], ['name' => $tableName]);
        $this->assertFalse($mock->hasTable($tableName));
        $this->assertTrue($mock->hasTable($tableName));
    }

    /**
     * @dataProvider columnsDataProvider
     */
    public function testGetColumnSqlDefinition(Column $column, string $expected)
    {
        $this->assertEquals($expected, (new SnowflakeAdapter([]))->getColumnSqlDefinition($column));
    }

    public function columnsDataProvider(): array
    {
        $precision = 9;
        $scale = 5;
        $limit = 42;
        $collation = 'en-cs';
        $seed = 111;
        $increment = 222;
        $data = [
            'number' => ['name' => 'field', 'type' => 'number', 'expected' => 'number null'],
            'decimal' => ['name' => 'field', 'type' => 'decimal', 'expected' => 'number null'],
            'numeric' => ['name' => 'field', 'type' => 'numeric', 'expected' => 'number null'],
            'int' => ['name' => 'field', 'type' => 'int', 'expected' => 'number null'],
            'integer' => ['name' => 'field', 'type' => 'integer', 'expected' => 'number null'],
            'bigint' => ['name' => 'field', 'type' => 'bigint', 'expected' => 'number null'],
            'smallint' => ['name' => 'field', 'type' => 'smallint', 'expected' => 'number null'],
            'tinyint' => ['name' => 'field', 'type' => 'tinyint', 'expected' => 'number null'],
            'byteint' => ['name' => 'field', 'type' => 'byteint', 'expected' => 'number null'],
            "number($precision,$scale)" => [
                'name' => 'field',
                'type' => 'number',
                'precision' => $precision,
                'scale' => $scale,
                'expected' => "number($precision,$scale) null"
            ],
            "number($precision,0)" => [
                'name' => 'field',
                'type' => 'number',
                'precision' => $precision,
                'expected' => "number($precision,0) null"
            ],
            'number default 7' => [
                'name' => 'field',
                'type' => 'number',
                'default' => 7,
                'expected' => 'number null default 7'
            ],
            "number($precision,$scale) default 42.42" => [
                'name' => 'field',
                'type' => 'number',
                'precision' => $precision,
                'scale' => $scale,
                'default' => '42.42',
                'expected' => "number($precision,$scale) null default 42.42"
            ],
            "integer with precision" => [
                'name' => 'field',
                'type' => 'integer',
                'precision' => $precision,
                'expected' => 'number null'
            ],
            'float' => ['name' => 'field', 'type' => 'float', 'expected' => 'float null'],
            'float4' => ['name' => 'field', 'type' => 'float4', 'expected' => 'float null'],
            'float8' => ['name' => 'field', 'type' => 'float8', 'expected' => 'float null'],
            'double' => ['name' => 'field', 'type' => 'double', 'expected' => 'float null'],
            'double precision' => ['name' => 'field', 'type' => 'double precision', 'expected' => 'float null'],
            'real' => ['name' => 'field', 'type' => 'real', 'expected' => 'float null'],
            'varchar' => ['name' => 'field', 'type' => 'varchar', 'expected' => 'varchar null'],
            'char' => ['name' => 'field', 'type' => 'char', 'expected' => 'varchar null'],
            'character' => ['name' => 'field', 'type' => 'character', 'expected' => 'varchar null'],
            'nchar' => ['name' => 'field', 'type' => 'nchar', 'expected' => 'varchar null'],
            'string' => ['name' => 'field', 'type' => 'string', 'expected' => 'varchar null'],
            'text' => ['name' => 'field', 'type' => 'text', 'expected' => 'varchar null'],
            'nvarchar' => ['name' => 'field', 'type' => 'nvarchar', 'expected' => 'varchar null'],
            'nvarchar2' => ['name' => 'field', 'type' => 'nvarchar2', 'expected' => 'varchar null'],
            'char varying' => ['name' => 'field', 'type' => 'char varying', 'expected' => 'varchar null'],
            'nchar varying' => ['name' => 'field', 'type' => 'nchar varying', 'expected' => 'varchar null'],
            "varchar($limit)" => [
                'name' => 'field',
                'type' => 'varchar',
                'limit' => $limit,
                'expected' => "varchar($limit) null"
            ],
            "varchar collate '$collation'" => [
                'name' => 'field',
                'type' => 'varchar',
                'collation' => $collation,
                'expected' => "varchar collate '$collation' null"
            ],
            "varchar($limit) collate '$collation'" => [
                'name' => 'field',
                'type' => 'varchar',
                'collation' => $collation,
                'limit' => $limit,
                'expected' => "varchar($limit) collate '$collation' null"
            ],
            "varchar default 'Lorem Ipsum'" => [
                'name' => 'field',
                'type' => 'varchar',
                'default' => 'Lorem Ipsum',
                'expected' => "varchar null default 'Lorem Ipsum'"
            ],
            "varchar default null" => [
                'name' => 'field',
                'type' => 'varchar',
                'default' => 'null',
                'expected' => "varchar null default null"
            ],
            'boolean' => ['name' => 'field', 'type' => 'boolean', 'expected' => 'boolean null'],
            'datetime' => ['name' => 'field', 'type' => 'datetime', 'expected' => 'timestamp_ntz null'],
            'time' => ['name' => 'field', 'type' => 'time', 'expected' => 'time null'],
            "time($precision)" => [
                'name' => 'field',
                'type' => 'time',
                'precision' => $precision,
                'expected' => "time($precision) null"
            ],
            'date' => ['name' => 'field', 'type' => 'date', 'expected' => 'date null'],
            'timestamp' => ['name' => 'field', 'type' => 'timestamp', 'expected' => 'timestamp null'],
            'timestamp_ltz' => ['name' => 'field', 'type' => 'timestamp_ltz', 'expected' => 'timestamp_ltz null'],
            'timestampltz' => ['name' => 'field', 'type' => 'timestampltz', 'expected' => 'timestamp_ltz null'],
            'timestamp with local time zone' => [
                'name' => 'field',
                'type' => 'timestamp with local time zone',
                'expected' => 'timestamp_ltz null'
            ],
            'timestamp_ntz' => ['name' => 'field', 'type' => 'timestamp_ntz', 'expected' => 'timestamp_ntz null'],
            'timestampntz' => ['name' => 'field', 'type' => 'timestampntz', 'expected' => 'timestamp_ntz null'],
            'timestamp without time zone' => [
                'name' => 'field',
                'type' => 'timestamp without time zone',
                'expected' => 'timestamp_ntz null'
            ],
            'timestamp_tz' => ['name' => 'field', 'type' => 'timestamp_tz', 'expected' => 'timestamp_tz null'],
            'timestamptz' => ['name' => 'field', 'type' => 'timestamptz', 'expected' => 'timestamp_tz null'],
            'timestamp with time zone' => [
                'name' => 'field',
                'type' => 'timestamp with time zone',
                'expected' => 'timestamp_tz null'
            ],
            'timestamp_ntz with function' => [
                'name' => 'field',
                'type' => 'timestamp_ntz',
                'default' => "convert_timezone('UTC', 'Europe/Budapest', sysdate())",
                'expected' => "timestamp_ntz null default convert_timezone('UTC', 'Europe/Budapest', sysdate())"
            ],
            'varchar not null' => [
                'name' => 'field',
                'type' => 'varchar',
                'null' => false,
                'expected' => 'varchar not null'
            ],
            'number identity' => [
                'name' => 'field',
                'type' => 'number',
                'identity' => true,
                'null' => false,
                'expected' => 'number identity not null'
            ],
            "number identity($seed,$increment)" => [
                'name' => 'field',
                'type' => 'number',
                'identity' => true,
                'seed' => $seed,
                'increment' => $increment,
                'expected' => "number identity($seed,$increment) null"
            ],
            'number with comment' => [
                'name' => 'field',
                'type' => 'number',
                'comment' => 'Lorem Ipsum',
                'expected' => "number null comment 'Lorem Ipsum'"
            ],
            'number with properties' => [
                'name' => 'field',
                'type' => 'number',
                'properties' => ['primary key'],
                'expected' => "number null primary key"
            ],
        ];
        $columns = [];
        foreach ($data as $testName => $item) {
            $column = [];
            $column[] = new Column();
            $column[] = $item['expected'];
            foreach (array_keys($item) as $key) {
                if ($key != 'expected') {
                    $column[0]->{'set' . ucfirst($key)}($item[$key]);
                }
            }
            $columns[$testName] = $column;
        }
        return $columns;
    }

    /**
     * @dataProvider createTableDataProvider
     */
    public function testCreateTable(Table $table, array $columns, array $indexes, string $expected)
    {
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['execute']);
        $mock->expects($this->once())->method('execute')->with($expected);
        $mock->createTable($table, $columns, $indexes);
    }

    public function createTableDataProvider(): array
    {
        $tables = [
            'my_awesome_table' => [
                'name' => 'my_awesome_table',
                'options' => ['primary_key' => ['lorem', 'ipsum']],
                'indexes' => [],
                'columns' => [
                    ['type' => 'number', 'name' => 'id', 'null' => false, 'identity' => true],
                    ['type' => 'varchar', 'name' => 'varchar', 'null' => false, 'identity' => false],
                    ['type' => 'datetime', 'name' => 'datetime', 'null' => false, 'identity' => false],
                ],
                'expected' =>
                    'create table "my_awesome_table" ("id" number identity not null, "varchar" varchar not null, "datetime" timestamp_ntz not null, primary key ("lorem", "ipsum"))',
            ],
            'phinxlog' => [
                'name' => 'phinxlog',
                'options' => ['id' => false, 'primary_key' => 'version'],
                'indexes' => [],
                'columns' => [
                    ['name' => 'version', 'type' => 'biginteger', 'null' => false],
                    ['name' => 'migration_name', 'type' => 'string', 'limit' => 100, 'default' => null, 'null' => true],
                    ['name' => 'start_time', 'type' => 'timestamp', 'default' => null, 'null' => true],
                    ['name' => 'end_time', 'type' => 'timestamp', 'default' => null, 'null' => true],
                    ['name' => 'breakpoint', 'type' => 'boolean', 'default' => false, 'null' => false]
                ],
                'expected' =>
                    'create table "phinxlog" ("version" number not null, "migration_name" varchar(100) null, "start_time" timestamp null, "end_time" timestamp null, "breakpoint" boolean not null default false, primary key ("version"))'
            ],
        ];
        $data = [];
        foreach ($tables as $testName => $t) {
            $table = [];
            $table['table'] = new Table($t['name'], $t['options']);
            foreach ($t['columns'] as $col) {
                $column = new Column();
                foreach ($col as $key => $value) {
                    $column->{'set' . ucfirst($key)}($value);
                }
                $table['columns'][] = $column;
            }
            $table['indexes'] = $t['indexes'];
            $table['expected'] = $t['expected'];
            $data[$testName] = $table;
        }
        return $data;
    }

    public function testHasColumn()
    {
        $tableName = 'table';
        $columnName = 'column';
        $expected = "show columns like '$columnName' in table \"$tableName\"";
        $statement = $this->createStub(\PDOStatement::class);
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['query']);
        $mock->expects($this->once())
            ->method('query')
            ->with($expected)
            ->willReturn($statement);
        $mock->hasColumn($tableName, $columnName);
    }

    /**
     * @dataProvider getVersionLogDataProvider
     */
    public function testGetVersionLog(array $options, $expected)
    {
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['fetchAll']);
        $mock->setOptions($options);
        $mock->expects($this->once())
            ->method('fetchAll')
            ->with($expected)
            ->willReturn([]);
        $mock->getVersionLog();
    }

    public function getVersionLogDataProvider(): array
    {
        return [
            Config::VERSION_ORDER_CREATION_TIME => [
                ['version_order' => Config::VERSION_ORDER_CREATION_TIME],
                'select * from "phinxlog" order by "version" asc'
            ],
            Config::VERSION_ORDER_EXECUTION_TIME => [
                ['version_order' => Config::VERSION_ORDER_EXECUTION_TIME],
                'select * from "phinxlog" order by "start_time" asc, "version" asc'
            ],
        ];
    }

}
