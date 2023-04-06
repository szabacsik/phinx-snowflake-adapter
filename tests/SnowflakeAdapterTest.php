<?php

namespace Szabacsik\Phinx\Tests;

use Phinx\Db\Table\Column;
use Phinx\Db\Table\ForeignKey;
use Phinx\Db\Table\Table;
use Phinx\Util\Literal;
use PHPUnit\Framework\TestCase;
use Szabacsik\Phinx\SnowflakeAdapter;
use Phinx\Config\Config;
use ReflectionObject;
use Phinx\Db\Util\AlterInstructions;
use InvalidArgumentException;
use PDO;
use PDOStatement;
use PHPUnit\Framework\MockObject\Exception;
use Symfony\Component\Console\Input\Input;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\Output;
use Symfony\Component\Console\Output\OutputInterface;
use ReflectionException;
use Phinx\Db\Table\Index;

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

    public static function columnsDataProvider(): array
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
            'timestamp with timezone option set to true' => [
                'name' => 'field',
                'type' => 'timestamp',
                'timezone' => true,
                'null' => false,
                'expected' => 'timestamp_tz not null'
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
            'number with primary key property' => [
                'name' => 'field',
                'type' => 'number',
                'properties' => ['primary key'],
                'expected' => "number null"
            ],
            'number with primary_key property' => [
                'name' => 'field',
                'type' => 'number',
                'properties' => ['primary_key'],
                'expected' => "number null"
            ],
            'number with properties' => [
                'name' => 'field',
                'type' => 'number',
                'properties' => ['lorem', 'ipsum'],
                'expected' => "number null lorem ipsum"
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
     * @throws Exception
     */
    public function testCreateTable(Table $table, array $columns, array $indexes, string $expected)
    {
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['execute']);
        $mock->expects($this->once())->method('execute')->with($expected);
        $mock->createTable($table, $columns, $indexes);
    }

    public static function createTableDataProvider(): array
    {
        $indexes[0] = new Index();
        $indexes[0]->setColumns(['column1', 'column2']);
        $indexes[0]->setType('unique');
        $indexes[0]->setName('');
        $indexes[1] = new Index();
        $indexes[1]->setColumns('column1');
        $indexes[1]->setType('unique');
        $indexes[1]->setName('unique_constraint_column1');
        $indexes[2] = new Index();
        $indexes[2]->setColumns('column2');
        $indexes[2]->setType('unique');
        $indexes[2]->setName('');
        $indexes[3] = new Index();
        $indexes[3]->setColumns(['column1', 'column2']);
        $indexes[3]->setType('unique');
        $indexes[3]->setName('unique_constraint_column1_and_column2');
        $indexes[4] = new Index();
        $indexes[4]->setColumns(['column2', 'column3']);
        $indexes[4]->setType('pRiMaRy-kEy');
        $indexes[4]->setName('primary key constraint');
        $tables = [
            'table with id column explicitly set' => [
                'name' => 'my_awesome_table',
                'options' => ['primary-key' => ['lorem', 'ipsum']],
                'indexes' => [],
                'columns' => [
                    ['type' => 'number', 'name' => 'id', 'null' => false, 'identity' => true, 'properties' => ['primary key']],
                    ['type' => 'varchar', 'name' => 'varchar', 'null' => false, 'identity' => false],
                    ['type' => 'datetime', 'name' => 'datetime', 'null' => false, 'identity' => false],
                ],
                'expected' =>
                    'create table "my_awesome_table" ("id" number identity not null, "varchar" varchar not null, "datetime" timestamp_ntz not null, primary key ("lorem", "ipsum"), primary key ("id"))',
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
            'table without id column set' => [
                'name' => 'my_awesome_table',
                'options' => [],
                'indexes' => [],
                'columns' => [
                    ['type' => 'number', 'name' => 'number', 'null' => false],
                    ['type' => 'varchar', 'name' => 'varchar', 'null' => false],
                    ['type' => 'datetime', 'name' => 'datetime', 'null' => false],
                ],
                'expected' =>
                    'create table "my_awesome_table" ("id" number identity not null, "number" number not null, "varchar" varchar not null, "datetime" timestamp_ntz not null, primary key ("id"))',
            ],
            'table with id column name set in options' => [
                'name' => 'my_awesome_table',
                'options' => ['id' => 'MyUniqueId'],
                'indexes' => [],
                'columns' => [
                    ['type' => 'number', 'name' => 'number', 'null' => false],
                    ['type' => 'varchar', 'name' => 'varchar', 'null' => false],
                    ['type' => 'datetime', 'name' => 'datetime', 'null' => false],
                ],
                'expected' =>
                    'create table "my_awesome_table" ("MyUniqueId" number identity not null, "number" number not null, "varchar" varchar not null, "datetime" timestamp_ntz not null, primary key ("MyUniqueId"))',
            ],
            'table using UUID for the primary id column' => [
                'name' => 'my_awesome_table',
                'options' => [],
                'indexes' => [],
                'columns' => [
                    ['type' => 'varchar', 'name' => 'id', 'limit' => 36, 'null' => false, 'properties' => ['primary key'], 'default' => 'uuid_string()'],
                    ['type' => 'number', 'name' => 'other'],
                ],
                'expected' =>
                    'create table "my_awesome_table" ("id" varchar(36) not null default uuid_string(), "other" number null, primary key ("id"))',
            ],
            'Let’s disable the automatic id column and create a primary key using two columns' => [
                'name' => 'followers',
                'options' => ['id' => false, 'primary_key' => ['user_id', 'follower_id']],
                'indexes' => [],
                'columns' => [
                    ['name' => 'user_id', 'type' => 'integer'],
                    ['name' => 'follower_id', 'type' => 'integer'],
                    ['name' => 'created', 'type' => 'datetime'],
                ],
                'expected' =>
                    'create table "followers" ("user_id" number null, "follower_id" number null, "created" timestamp_ntz null, primary key ("user_id", "follower_id"))',
            ],
            'Add unique constraint' => [
                'name' => 'table',
                'options' => ['id' => false],
                'indexes' => [$indexes[0]],
                'columns' => [
                    ['name' => 'column1', 'type' => 'integer'],
                    ['name' => 'column2', 'type' => 'integer'],
                    ['name' => 'created', 'type' => 'datetime'],
                ],
                'expected' =>
                    'create table "table" ("column1" number null, "column2" number null, "created" timestamp_ntz null, unique ("column1", "column2"))',
            ],
            'Some indexes' => [
                'name' => 'table',
                'options' => ['unique' => ['column1', 'column2']],
                'indexes' => $indexes,
                'columns' => [
                    ['name' => 'column1', 'type' => 'number'],
                    ['name' => 'column2', 'type' => 'number'],
                    ['name' => 'column3', 'type' => 'number'],
                ],
                'expected' =>
                    'create table "table" (' .
                    '"id" number identity not null, ' .
                    '"column1" number null, "column2" number null, "column3" number null, ' .
                    'primary key ("id"), ' .
                    'unique ("column1", "column2"), ' .
                    'constraint "unique_constraint_column1" unique ("column1"), ' .
                    'unique ("column2"), ' .
                    'constraint "unique_constraint_column1_and_column2" unique ("column1", "column2"), ' .
                    'constraint "primary key constraint" primary key ("column2", "column3")' .
                    ')',
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
    public function testGetVersionLog(array $options, array $rows, string $expected)
    {
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['fetchAll']);
        $mock->setOptions($options);
        $mock->expects($this->once())
            ->method('fetchAll')
            ->with($expected)
            ->willReturn($rows);
        $versionLog = $mock->getVersionLog();
        $this->assertCount(count($rows), $versionLog);
        foreach ($rows as $row) {
            if (in_array($row['breakpoint'], [1, '1', 'true', true], true)) {
                $this->assertEquals(1, $versionLog[$row['version']]['breakpoint'], 'Failed breakpoint with version: ' . $row['version'] . '.');
            } else {
                $this->assertEquals(0, $versionLog[$row['version']]['breakpoint'], 'Failed breakpoint with version: ' . $row['version'] . '.');
            }
        }
    }

    public static function getVersionLogDataProvider(): array
    {
        $rows = [
            [
                'version' => '10001',
                'breakpoint' => 0,
            ],
            [
                'version' => '10002',
                'breakpoint' => '0',
            ],
            [
                'version' => '10003',
                'breakpoint' => '',
            ],
            [
                'version' => '10004',
                'breakpoint' => null,
            ],
            [
                'version' => '10005',
                'breakpoint' => false,
            ],
            [
                'version' => '10006',
                'breakpoint' => 1,
            ],
            [
                'version' => '10007',
                'breakpoint' => '1',
            ],
            [
                'version' => '10008',
                'breakpoint' => true,
            ],
            [
                'version' => '10009',
                'breakpoint' => 'true',
            ],
        ];
        return [
            Config::VERSION_ORDER_CREATION_TIME => [
                ['version_order' => Config::VERSION_ORDER_CREATION_TIME],
                $rows,
                'select * from "phinxlog" order by "version" asc'
            ],
            Config::VERSION_ORDER_EXECUTION_TIME => [
                ['version_order' => Config::VERSION_ORDER_EXECUTION_TIME],
                $rows,
                'select * from "phinxlog" order by "start_time" asc, "version" asc'
            ],
        ];
    }

    /**
     * @dataProvider columnTypeDataProvider
     */
    public function testIsValidColumnType(Column $column, $valid)
    {
        $adapter = new SnowflakeAdapter([]);
        $this->assertEquals($valid, $adapter->isValidColumnType($column));
    }

    public function testGetColumnTypes()
    {
        $adapter = new SnowflakeAdapter([]);
        $filtered = array_filter(static::columnTypeDataProvider(), function ($item) {
            return $item['valid'] === true;
        });
        $expected = array_keys($filtered);
        $this->assertSame($expected, $adapter->getColumnTypes());
    }

    public static function columnTypeDataProvider(): array
    {
        $createColumnAndSetType = fn($type) => (new Column)->setType($type);
        return [
            'number' => [
                'column' => $createColumnAndSetType('number'),
                'valid' => true,
            ],
            'decimal' => [
                'column' => $createColumnAndSetType('decimal'),
                'valid' => true,
            ],
            'numeric' => [
                'column' => $createColumnAndSetType('numeric'),
                'valid' => true,
            ],
            'int' => [
                'column' => $createColumnAndSetType('int'),
                'valid' => true,
            ],
            'integer' => [
                'column' => $createColumnAndSetType('integer'),
                'valid' => true,
            ],
            'bigint' => [
                'column' => $createColumnAndSetType('bigint'),
                'valid' => true,
            ],
            'smallint' => [
                'column' => $createColumnAndSetType('smallint'),
                'valid' => true,
            ],
            'tinyint' => [
                'column' => $createColumnAndSetType('tinyint'),
                'valid' => true,
            ],
            'byteint' => [
                'column' => $createColumnAndSetType('byteint'),
                'valid' => true,
            ],
            'float' => [
                'column' => $createColumnAndSetType('float'),
                'valid' => true,
            ],
            'float4' => [
                'column' => $createColumnAndSetType('float4'),
                'valid' => true,
            ],
            'float8' => [
                'column' => $createColumnAndSetType('float8'),
                'valid' => true,
            ],
            'double' => [
                'column' => $createColumnAndSetType('double'),
                'valid' => true,
            ],
            'double precision' => [
                'column' => $createColumnAndSetType('double precision'),
                'valid' => true,
            ],
            'real' => [
                'column' => $createColumnAndSetType('real'),
                'valid' => true,
            ],
            'biginteger' => [
                'column' => $createColumnAndSetType('biginteger'),
                'valid' => true,
            ],
            'varchar' => [
                'column' => $createColumnAndSetType('varchar'),
                'valid' => true,
            ],
            'char' => [
                'column' => $createColumnAndSetType('char'),
                'valid' => true,
            ],
            'character' => [
                'column' => $createColumnAndSetType('character'),
                'valid' => true,
            ],
            'string' => [
                'column' => $createColumnAndSetType('string'),
                'valid' => true,
            ],
            'text' => [
                'column' => $createColumnAndSetType('text'),
                'valid' => true,
            ],
            'binary' => [
                'column' => $createColumnAndSetType('binary'),
                'valid' => true,
            ],
            'varbinary' => [
                'column' => $createColumnAndSetType('varbinary'),
                'valid' => true,
            ],
            'boolean' => [
                'column' => $createColumnAndSetType('boolean'),
                'valid' => true,
            ],
            'date' => [
                'column' => $createColumnAndSetType('date'),
                'valid' => true,
            ],
            'datetime' => [
                'column' => $createColumnAndSetType('datetime'),
                'valid' => true,
            ],
            'time' => [
                'column' => $createColumnAndSetType('time'),
                'valid' => true,
            ],
            'timestamp' => [
                'column' => $createColumnAndSetType('timestamp'),
                'valid' => true,
            ],
            'timestamp_ltz' => [
                'column' => $createColumnAndSetType('timestamp_ltz'),
                'valid' => true,
            ],
            'timestamp_ntz' => [
                'column' => $createColumnAndSetType('timestamp_ntz'),
                'valid' => true,
            ],
            'timestamp_tz' => [
                'column' => $createColumnAndSetType('timestamp_tz'),
                'valid' => true,
            ],
            'variant' => [
                'column' => $createColumnAndSetType('variant'),
                'valid' => true,
            ],
            'object' => [
                'column' => $createColumnAndSetType('object'),
                'valid' => true,
            ],
            'array' => [
                'column' => $createColumnAndSetType('array'),
                'valid' => true,
            ],
            'geography' => [
                'column' => $createColumnAndSetType('geography'),
                'valid' => true,
            ],
            'geometry' => [
                'column' => $createColumnAndSetType('geometry'),
                'valid' => true,
            ],
            'invalid' => [
                'column' => $createColumnAndSetType('invalid'),
                'valid' => false,
            ],
        ];
    }

    public function testGetDropTableInstructions()
    {
        $tableName = 'table';
        $adapter = new SnowflakeAdapter([]);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getDropTableInstructions');
        $method->setAccessible(true);
        $alterInstructions = $method->invoke($adapter, $tableName);
        $this->assertInstanceOf(AlterInstructions::class, $alterInstructions);
        $this->assertCount(1, $alterInstructions->getPostSteps());
        $this->assertSame("drop table \"$tableName\"", $alterInstructions->getPostSteps()[0]);
    }

    public function testGetAddColumnInstructions()
    {
        $tableName = 'table';
        $columnName = 'column';
        $columnType = 'varchar';
        $table = new Table($tableName);
        $column = new Column();
        $column->setName($columnName);
        $column->setType($columnType);
        $adapter = new SnowflakeAdapter([]);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getAddColumnInstructions');
        $method->setAccessible(true);
        $alterInstructions = $method->invoke($adapter, $table, $column);
        $this->assertInstanceOf(AlterInstructions::class, $alterInstructions);
        $this->assertCount(1, $alterInstructions->getAlterParts());
        $this->assertEquals("add \"$columnName\" $columnType null", $alterInstructions->getAlterParts()[0]);
    }

    public function testGetDropColumnInstructions()
    {
        $tableName = 'table';
        $columnName = 'column';
        $adapter = new SnowflakeAdapter([]);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getDropColumnInstructions');
        $alterInstructions = $method->invoke($adapter, $tableName, $columnName);
        $this->assertInstanceOf(AlterInstructions::class, $alterInstructions);
        $this->assertCount(1, $alterInstructions->getAlterParts());
        $this->assertEquals("drop column \"$columnName\"", $alterInstructions->getAlterParts()[0]);
    }

    public function testGetRenameColumnInstructions()
    {
        $tableName = 'table';
        $columnName = 'column';
        $newColumnName = 'new_column';
        $adapter = new SnowflakeAdapter([]);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getRenameColumnInstructions');
        $alterInstructions = $method->invoke($adapter, $tableName, $columnName, $newColumnName);
        $this->assertInstanceOf(AlterInstructions::class, $alterInstructions);
        $this->assertCount(1, $alterInstructions->getAlterParts());
        $this->assertEquals("rename column \"$columnName\" to \"$newColumnName\"", $alterInstructions->getAlterParts()[0]);
    }

    public function testGetRenameTableInstructions()
    {
        $tableName = 'table';
        $newTableName = 'new_table';
        $adapter = new SnowflakeAdapter([]);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getRenameTableInstructions');
        $alterInstructions = $method->invoke($adapter, $tableName, $newTableName);
        $this->assertInstanceOf(AlterInstructions::class, $alterInstructions);
        $this->assertCount(1, $alterInstructions->getAlterParts());
        $this->assertEquals("rename to \"$newTableName\"", $alterInstructions->getAlterParts()[0]);
    }

    /**
     * @dataProvider getChangePrimaryKeyInstructionsDataProvider
     */
    public function testGetChangePrimaryKeyInstructions($table, $newColumns, $expected)
    {
        $adapter = $this->createPartialMock(SnowflakeAdapter::class, ['hasPrimaryKey']);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getChangePrimaryKeyInstructions');
        if (isset($expected['exception'])) {
            $this->expectException($expected['exception']);
        } else {
            $adapter->expects($this->once())->method('hasPrimaryKey')->willReturn(true);
        }
        $alterInstructions = $method->invoke($adapter, $table, $newColumns);
        if (isset($expected['sql'])) {
            $this->assertCount(count($expected['sql']), $alterInstructions->getAlterParts());
            foreach ($expected['sql'] as $index => $sql) {
                $this->assertEquals($sql, $alterInstructions->getAlterParts()[$index]);
            }
        }
    }

    public static function getChangePrimaryKeyInstructionsDataProvider(): array
    {
        return [
            '`newColumns` type is invalid' => [
                'table' => new Table('table'),
                'newColumns' => 42,
                'expected' => [
                    'exception' => \InvalidArgumentException::class,
                    'sql' => [],
                ]
            ],
            '`newColumns` array element with invalid type' => [
                'table' => new Table('table'),
                'newColumns' => ['lorem', 42],
                'expected' => [
                    'exception' => \InvalidArgumentException::class,
                    'sql' => [],
                ]
            ],
            '`newColumns` is null' => [
                'table' => new Table('table'),
                'newColumns' => null,
                'expected' => [
                    'sql' => [
                        'drop primary key'
                    ],
                ]
            ],
            '`newColumns` is string' => [
                'table' => new Table('table'),
                'newColumns' => 'column',
                'expected' => [
                    'sql' => [
                        'drop primary key',
                        'add primary key ("column")'
                    ],
                ]
            ],
            '`newColumns` is array of strings' => [
                'table' => new Table('table'),
                'newColumns' => ['column1', 'column2', 'column3'],
                'expected' => [
                    'sql' => [
                        'drop primary key',
                        'add primary key ("column1","column2","column3")'
                    ],
                ]
            ],
        ];
    }

    /**
     * @dataProvider executeAlterStepsDataProvider
     */
    public function testExecuteAlterSteps(string $tableName, AlterInstructions $instructions, array $expected)
    {
        $adapter = $this->createPartialMock(SnowflakeAdapter::class, ['execute']);
        $reflection = new ReflectionObject($adapter);
        $executeAlterStepsMethod = $reflection->getMethod('executeAlterSteps');
        $executeMethodParameters = [];
        $adapter
            ->expects($this->exactly(count($expected)))
            ->method('execute')
            ->willReturnCallback(function (...$args) use (&$executeMethodParameters) {
                $executeMethodParameters[] = $args;
                return 1;
            });
        $executeAlterStepsMethod->invoke($adapter, $tableName, $instructions);
        foreach ($expected as $index => $sql) {
            $this->assertEquals($sql, $executeMethodParameters[$index][0]);
        }
    }

    public static function executeAlterStepsDataProvider(): array
    {
        $tableName = 'table';
        $newColumns = ['column1', 'column2', 'column3'];
        $newColumn = 'column';
        $table = new Table($tableName);
        $adapter = \Mockery::mock(SnowflakeAdapter::class . '[hasPrimaryKey]', [[]]);
        $adapter->shouldReceive('hasPrimaryKey')->andReturn(true);
        $reflection = new ReflectionObject($adapter);
        $getChangePrimaryKeyInstructionsMethod = $reflection->getMethod('getChangePrimaryKeyInstructions');
        $changePrimaryKeyWithoutColumnInstructions = $getChangePrimaryKeyInstructionsMethod->invoke($adapter, $table, null);
        $changePrimaryKeySingleColumnInstructions = $getChangePrimaryKeyInstructionsMethod->invoke($adapter, $table, $newColumn);
        $changePrimaryKeyManyColumnsInstructions = $getChangePrimaryKeyInstructionsMethod->invoke($adapter, $table, $newColumns);
        return [
            'change primary key, without column' => [
                'tableName' => $tableName,
                'instructions' => $changePrimaryKeyWithoutColumnInstructions,
                'expected' => [
                    sprintf('alter table "%s" drop primary key', $tableName),
                ]
            ],
            'change primary key, single column' => [
                'tableName' => $tableName,
                'instructions' => $changePrimaryKeySingleColumnInstructions,
                'expected' => [
                    sprintf('alter table "%s" drop primary key', $tableName),
                    sprintf('alter table "%s" add primary key ("%s")', $tableName, $newColumn),
                ]
            ],
            'change primary key, many columns' => [
                'tableName' => $tableName,
                'instructions' => $changePrimaryKeyManyColumnsInstructions,
                'expected' => [
                    sprintf('alter table "%s" drop primary key', $tableName),
                    sprintf('alter table "%s" add primary key ("%s")', $tableName, implode('","', $newColumns)),
                ]
            ],
            'instruction with string post steps' => [
                'tableName' => $tableName,
                'instructions' => new AlterInstructions([], ['lorem', 'ipsum']),
                'expected' => ['lorem', 'ipsum']
            ],
        ];
    }

    public function testHasPrimaryKey()
    {
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['fetchAll']);
        $mock->expects($this->exactly(2))
            ->method('fetchAll')
            ->with('show primary keys in "table"')
            ->willReturnOnConsecutiveCalls([], [['column_name' => 'column1'], ['column_name' => 'column2']]);
        $this->assertFalse($mock->hasPrimaryKey('table', []));
        $this->assertTrue($mock->hasPrimaryKey('table', []));
    }

    public function testGetChangeCommentInstructions()
    {
        $table = new Table('table');
        $comment = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.';
        $expected = sprintf("comment on table \"%s\" is '%s'", $table->getName(), $comment);
        $adapter = new SnowflakeAdapter([]);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getChangeCommentInstructions');
        $instructions = $method->invoke($adapter, $table, $comment);
        $this->assertInstanceOf(AlterInstructions::class, $instructions);
        $this->assertEquals([], $instructions->getAlterParts());
        $this->assertEquals([$expected], $instructions->getPostSteps());
    }

    public function testTruncateTable()
    {
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['execute']);
        $mock->expects($this->once())->method('execute')->with('truncate table "table"');
        $mock->truncateTable('table');
    }

    /**
     * @dataProvider getAddForeignKeyInstructionsDataProvider
     */
    public function testGetAddForeignKeyInstructions(Table $table, ForeignKey $foreignKey, array $expected): void
    {
        $adapter = new SnowflakeAdapter([]);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getAddForeignKeyInstructions');
        $instructions = $method->invoke($adapter, $table, $foreignKey);
        $this->assertInstanceOf(AlterInstructions::class, $instructions);
        $this->assertEquals($expected, $instructions->getAlterParts());
    }

    public static function getAddForeignKeyInstructionsDataProvider(): array
    {
        $table = new Table('table');
        $referencedTable = new Table('referencedTable');
        $foreignKey1 = new ForeignKey();
        $foreignKey1->setOptions([]);
        $foreignKey1->setOnUpdate('NO_ACTION');
        $foreignKey1->setOnDelete('NO_ACTION');
        $foreignKey1->setConstraint('');
        $foreignKey1->setColumns('column1');
        $foreignKey1->setReferencedColumns(['referencedColumn1']);
        $foreignKey1->setReferencedTable($referencedTable);

        $foreignKey2 = new ForeignKey();
        $foreignKey2->setOptions([]);
        $foreignKey2->setOnUpdate('NO_ACTION');
        $foreignKey2->setOnDelete('NO_ACTION');
        $foreignKey2->setConstraint('myConstraint');
        $foreignKey2->setColumns('column1');
        $foreignKey2->setReferencedColumns(['referencedColumn1']);
        $foreignKey2->setReferencedTable($referencedTable);

        $foreignKey3 = new ForeignKey();
        $foreignKey3->setOptions([]);
        $foreignKey3->setOnUpdate('NO_ACTION');
        $foreignKey3->setOnDelete('NO_ACTION');
        $foreignKey3->setConstraint('');
        $foreignKey3->setColumns(['column1', 'column2']);
        $foreignKey3->setReferencedColumns(['referencedColumn1', 'referencedColumn2']);
        $foreignKey3->setReferencedTable($referencedTable);
        return [
            'columns is string, referenceColumns contains a string, constraint is empty' => [
                'table' => $table,
                'foreignKey' => $foreignKey1,
                'expected' => ['add foreign key ("column1") references "referencedTable"("referencedColumn1")']
            ],
            'columns is string, referenceColumns contains a string, constraint is specified' => [
                'table' => $table,
                'foreignKey' => $foreignKey2,
                'expected' => ['add constraint "myConstraint" foreign key ("column1") references "referencedTable"("referencedColumn1")']
            ],
            'columns is array, referenceColumns contains two strings, constraint is empty' => [
                'table' => $table,
                'foreignKey' => $foreignKey3,
                'expected' => ['add foreign key ("column1","column2") references "referencedTable"("referencedColumn1","referencedColumn2")']
            ],
        ];
    }

    public function testGetDropForeignKeyByColumnsInstructions()
    {
        $tableName = 'table';
        $columns = ['column1', 'column2'];
        $expected = 'drop foreign key ("column1","column2")';
        $adapter = new SnowflakeAdapter([]);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getDropForeignKeyByColumnsInstructions');
        $instructions = $method->invoke($adapter, $tableName, $columns);
        $this->assertEquals($expected, $instructions->getAlterParts()[0]);
    }

    public function testTableAddColumn()
    {
        $adapter = new SnowflakeAdapter([]);
        $table = new \Phinx\Db\Table('table', [], $adapter);
        $types = [
            'decimal',
            'numeric',
            'int',
            'integer',
            'bigint',
            'smallint',
            'tinyint',
            'byteint',
            'float',
            'float4',
            'float8',
            'double',
            'double precision',
            'real',
            'biginteger',
            'varchar',
            'char',
            'character',
            'string',
            'text',
            'binary',
            'varbinary',
            'boolean',
            'date',
            'datetime',
            'time',
            'timestamp',
            'timestamp_ltz',
            'timestamp_ntz',
            'timestamp_tz',
            'variant',
            'object',
            'array',
            'geography',
            'geometry',
        ];
        try {
            foreach ($types as $type) {
                $name = $type;
                $table->addColumn($name, $type);
            }
        } catch (\Exception $exception) {
            $this->fail(sprintf('%s: %s', get_class($exception), $exception->getMessage()));
        }
        $this->expectException(InvalidArgumentException::class);
        $table->addColumn('name', 'invalid_type');
    }

    /**
     * @dataProvider getColumnsDataProvider
     */
    public function testGetColumns(array $rows, Column $column)
    {
        $tableName = 'table';
        $mock = $this->createPartialMock(SnowflakeAdapter::class, ['fetchAll']);
        $mock->expects($this->once())
            ->method('fetchAll')
            ->with(sprintf('show columns in table "%s"', $tableName))
            ->willReturn($rows);
        $columns = $mock->getColumns($tableName);
        $this->assertCount(1, $columns);
        $this->assertEquals($column->getName(), $columns[0]->getName(), 'Name');
        $this->assertEquals($column->getType(), $columns[0]->getType(), 'Type');
        $this->assertEquals($column->getPrecision(), $columns[0]->getPrecision(), 'Precision');
        $this->assertEquals($column->getLimit(), $columns[0]->getLimit(), 'Limit');
        $this->assertEquals($column->getScale(), $columns[0]->getScale(), 'Scale');
        $this->assertEquals($column->getNull(), $columns[0]->getNull(), 'Nullable');
        $this->assertEquals($column->getDefault(), $columns[0]->getDefault(), 'Default');
        $this->assertEquals($column->getComment(), $columns[0]->getComment(), 'Comment');
        $this->assertEquals($column->getIdentity(), $columns[0]->getIdentity(), 'Identity');
    }

    public static function getColumnsDataProvider(): array
    {
        $column1 = new Column();
        $column1->setName('column1');
        $column1->setType('number');
        $column1->setPrecision(38);
        $column1->setScale(0);
        $column1->setNull(false);
        $column1->setDefault('');
        $column1->setComment('Lorem ipsum dolor sit amet');
        $column1->setIdentity(true);
        $column1->setSeed(100);
        $column1->setIncrement(10);

        $column2 = new Column();
        $column2->setName('column2');
        $column2->setType('number');
        $column2->setPrecision(38);
        $column2->setScale(0);
        $column2->setNull(false);
        $column2->setDefault('');
        $column2->setComment('Lorem ipsum dolor sit amet');
        $column2->setIdentity(true);
        $column2->setSeed(10);
        $column2->setIncrement(20);

        $column3 = new Column();
        $column3->setName('column3');
        $column3->setType('number');
        $column3->setPrecision(38);
        $column3->setScale(0);
        $column3->setNull(false);
        $column3->setDefault('');
        $column3->setComment('');
        $column3->setIdentity(false);

        $column4 = new Column();
        $column4->setName('column4');
        $column4->setType('number');
        $column4->setPrecision(5);
        $column4->setScale(3);
        $column4->setNull(true);
        $column4->setDefault(5.2);
        $column4->setComment('');

        $column5 = new Column();
        $column5->setName('column5');
        $column5->setType('number');
        $column5->setPrecision(38);
        $column5->setScale(0);
        $column5->setNull(true);
        $column5->setDefault('');
        $column5->setComment('');

        $column6 = new Column();
        $column6->setName('column6');
        $column6->setType('varchar');
        $column6->setLimit(16777216);
        $column6->setNull(true);
        $column6->setDefault('');
        $column6->setComment('');

        $column7 = new Column();
        $column7->setName('column7');
        $column7->setType('varchar');
        $column7->setLimit(16777216);
        $column7->setNull(true);
        $column7->setDefault('CAST(CAST(CONVERT_TIMEZONE(\'UTC\', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)) AS VARCHAR(16777216))');
        $column7->setComment('');

        $column8 = new Column();
        $column8->setName('column8');
        $column8->setType('varchar');
        $column8->setLimit(10);
        $column8->setNull(true);
        $column8->setCollation('en-cs');
        $column8->setDefault('');
        $column8->setComment('');

        $column9 = new Column();
        $column9->setName('column9');
        $column9->setType('varchar');
        $column9->setLimit(10);
        $column9->setNull(false);
        $column9->setCollation('en-cs');
        $column9->setDefault('none');
        $column9->setComment('');

        $column10 = new Column();
        $column10->setName('column10');
        $column10->setType('timestamp_ntz');
        $column10->setPrecision(0);
        $column10->setScale(9);
        $column10->setNull(true);
        $column10->setDefault('CONVERT_TIMEZONE(\'UTC\', \'Europe/Budapest\', CAST(CONVERT_TIMEZONE(\'UTC\', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)))');

        $column11 = new Column();
        $column11->setName('column11');
        $column11->setType('timestamp_ntz');
        $column11->setPrecision(0);
        $column11->setScale(9);
        $column11->setNull(true);
        $column11->setDefault('CAST(\'0001-01-01 00:00:00.000000000\' AS TIMESTAMP_NTZ(9))');

        $column12 = new Column();
        $column12->setName('column12');
        $column12->setType('timestamp_ntz');
        $column12->setPrecision(0);
        $column12->setScale(9);
        $column12->setNull(true);
        $column12->setDefault('CURRENT_TIMESTAMP()');

        $column13 = new Column();
        $column13->setName('column13');
        $column13->setType('timestamp_ltz');
        $column13->setPrecision(0);
        $column13->setScale(9);
        $column13->setNull(true);
        $column13->setDefault('CAST(\'0001-01-01 00:00:00.000000000\' AS TIMESTAMP_NTZ(9))');

        $column14 = new Column();
        $column14->setName('column14');
        $column14->setType('timestamp_tz');
        $column14->setPrecision(0);
        $column14->setScale(9);
        $column14->setNull(true);
        $column14->setDefault('CAST(\'0001-01-01 00:00:00.000000000 +0100\' AS TIMESTAMP_NTZ(9))');

        $column15 = new Column();
        $column15->setName('column15');
        $column15->setType('timestamp_ntz');
        $column15->setPrecision(0);
        $column15->setScale(9);
        $column15->setNull(false);

        $column16 = new Column();
        $column16->setName('column16');
        $column16->setType('boolean');
        $column16->setNull(true);
        $column16->setDefault(false);

        $column17 = new Column();
        $column17->setName('column17');
        $column17->setType('boolean');
        $column17->setNull(true);
        $column17->setDefault(true);

        $column18 = new Column();
        $column18->setName('column18');
        $column18->setType('float');
        $column18->setNull(true);

        return [
            'column1' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column1',
                    'data_type' => '{"type":"FIXED","precision":38,"scale":0,"nullable":false}',
                    'null?' => 'NOT_NULL',
                    'default' => '',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => 'Lorem ipsum dolor sit amet',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => 'IDENTITY START 100 INCREMENT 10',
                ]],
                'expected' => $column1
            ],
            'column2' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column2',
                    'data_type' => '{"type":"FIXED","precision":38,"scale":0,"nullable":false}',
                    'null?' => 'NOT_NULL',
                    'default' => '',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => 'Lorem ipsum dolor sit amet',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => 'IDENTITY START 10 INCREMENT 20',
                ]],
                'expected' => $column2,
            ],
            'column3' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column3',
                    'data_type' => '{"type":"FIXED","precision":38,"scale":0,"nullable":false}',
                    'null?' => 'NOT_NULL',
                    'default' => '',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column3,
            ],
            'column4' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column4',
                    'data_type' => '{"type":"FIXED","precision":5,"scale":3,"nullable":true}',
                    'null?' => 'true',
                    'default' => 5.2,
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column4,
            ],
            'column5' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column5',
                    'data_type' => '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    'null?' => 'true',
                    'default' => '',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column5,
            ],
            'column6' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column6',
                    'data_type' => '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                    'null?' => 'true',
                    'default' => '',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column6,
            ],
            'column7' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column7',
                    'data_type' => '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                    'null?' => 'true',
                    'default' => 'CAST(CAST(CONVERT_TIMEZONE(\'UTC\', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)) AS VARCHAR(16777216))',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column7,
            ],
            'column8' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column8',
                    'data_type' => '{"type":"TEXT","length":10,"byteLength":40,"nullable":true,"fixed":false,"collation":"en-cs"}',
                    'null?' => 'true',
                    'default' => '',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column8,
            ],
            'column9' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column9',
                    'data_type' => '{"type":"TEXT","length":10,"byteLength":40,"nullable":false,"fixed":false,"collation":"en-cs"}',
                    'null?' => 'NOT_NULL',
                    'default' => '\'none\'',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column9,
            ],
            'column10' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column10',
                    'data_type' => '{"type":"TIMESTAMP_NTZ","precision":0,"scale":9,"nullable":true}',
                    'null?' => 'true',
                    'default' => 'CONVERT_TIMEZONE(\'UTC\', \'Europe/Budapest\', CAST(CONVERT_TIMEZONE(\'UTC\', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_TZ(9))) AS TIMESTAMP_NTZ(9)))',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column10,
            ],
            'column11' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column11',
                    'data_type' => '{"type":"TIMESTAMP_NTZ","precision":0,"scale":9,"nullable":true}',
                    'null?' => 'true',
                    'default' => 'CAST(\'0001-01-01 00:00:00.000000000\' AS TIMESTAMP_NTZ(9))',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column11,
            ],
            'column12' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column12',
                    'data_type' => '{"type":"TIMESTAMP_NTZ","precision":0,"scale":9,"nullable":true}',
                    'null?' => 'true',
                    'default' => 'CURRENT_TIMESTAMP()',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column12,
            ],
            'column13' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column13',
                    'data_type' => '{"type":"TIMESTAMP_LTZ","precision":0,"scale":9,"nullable":true}',
                    'null?' => 'true',
                    'default' => 'CAST(\'0001-01-01 00:00:00.000000000\' AS TIMESTAMP_NTZ(9))',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column13,
            ],
            'column14' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column14',
                    'data_type' => '{"type":"TIMESTAMP_TZ","precision":0,"scale":9,"nullable":true}',
                    'null?' => 'true',
                    'default' => 'CAST(\'0001-01-01 00:00:00.000000000 +0100\' AS TIMESTAMP_NTZ(9))',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column14,
            ],
            'column15' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column15',
                    'data_type' => '{"type":"TIMESTAMP_NTZ","precision":0,"scale":9,"nullable":false}',
                    'null?' => 'NOT_NULL',
                    'default' => '',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column15,
            ],
            'column16' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column16',
                    'data_type' => '{"type":"BOOLEAN","nullable":true}',
                    'null?' => 'true',
                    'default' => 'FALSE',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column16,
            ],
            'column17' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column17',
                    'data_type' => '{"type":"BOOLEAN","nullable":true}',
                    'null?' => 'true',
                    'default' => 'TRUE',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column17,
            ],
            'column18' => [
                'rows' => [[
                    'table_name' => 'allin',
                    'schema_name' => 'TEST_SCHEMA',
                    'column_name' => 'column18',
                    'data_type' => '{"type":"REAL","nullable":true}',
                    'null?' => 'true',
                    'default' => '',
                    'kind' => 'COLUMN',
                    'expression' => '',
                    'comment' => '',
                    'database_name' => 'TEST_DATABASE',
                    'autoincrement' => '',
                ]],
                'expected' => $column18,
            ],
        ];
    }

    /**
     * @see https://docs.snowflake.com/en/sql-reference/sql/alter-table-column
     * @dataProvider getChangeColumnInstructionsDataProvider
     */

    public function testGetChangeColumnInstructions(Column $newColumn, array $columns, AlterInstructions $expectedInstructions)
    {
        $tableName = 'table';
        $columnName = $newColumn->getName();
        $adapter = $this->createPartialMock(SnowflakeAdapter::class, ['getColumns']);
        $adapter->expects($this->once())->method('getColumns')->willReturn($columns);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getChangeColumnInstructions');
        $instructions = $method->invoke($adapter, $tableName, $columnName, $newColumn);
        $this->assertInstanceOf(AlterInstructions::class, $instructions);
        $this->assertEquals($expectedInstructions->getAlterParts(), $instructions->getAlterParts());
    }

    public static function getChangeColumnInstructionsDataProvider(): array
    {
        $tests = [];
        $columnName = 'column';

        //Drop the default for a column
        $currentColumn = new Column();
        $currentColumn->setName($columnName);
        $currentColumn->setType('varchar');
        $currentColumn->setDefault('default');
        $newColumn = new Column();
        $newColumn->setName($columnName);
        $newColumn->setType('varchar');
        $newColumn->setDefault(null);
        $instructions = new AlterInstructions([sprintf(
            'alter column "%s" drop default', $columnName)
        ]);
        $tests['Drop the default for a column'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        //Change the default sequence for a column (i.e. SET DEFAULT seq_name.NEXTVAL)
        $currentColumn = new Column();
        $currentColumn->setName($columnName);
        $currentColumn->setType('number');
        $currentColumn->setDefault('sequence_01.nextval');
        $newColumn = new Column();
        $newColumn->setName($columnName);
        $newColumn->setType('number');
        $newColumn->setDefault('sequence_02.nextval');
        $instructions = new AlterInstructions([sprintf(
            'alter column "%s" set default sequence_02.nextval', $columnName)
        ]);
        $tests['Change the default sequence for a column'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        //Change the nullability of a column (i.e. SET NOT NULL or DROP NOT NULL)
        $currentColumn = new Column();
        $currentColumn->setName($columnName);
        $currentColumn->setType('varchar');
        $currentColumn->setNull(true);
        $newColumn = new Column();
        $newColumn->setName($columnName);
        $newColumn->setType('varchar');
        $newColumn->setNull(false);
        $instructions = new AlterInstructions([
            sprintf('alter column "%s" set not null', $columnName)
        ]);
        $tests['Change the nullability of a column (SET)'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];
        $currentColumn = new Column();
        $currentColumn->setName($columnName);
        $currentColumn->setType('varchar');
        $currentColumn->setNull(false);
        $newColumn = new Column();
        $newColumn->setName($columnName);
        $newColumn->setType('varchar');
        $newColumn->setNull(true);
        $instructions = new AlterInstructions([
            sprintf('alter column "%s" drop not null', $columnName)
        ]);
        $tests['Change the nullability of a column (DROP)'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        //Change a column data type to a synonymous type (e.g. STRING to VARCHAR).
        $currentColumn = new Column();
        $currentColumn->setName($columnName);
        $currentColumn->setType('string');
        $newColumn = new Column();
        $newColumn->setName($columnName);
        $newColumn->setType('varchar');
        $instructions = new AlterInstructions([
            sprintf('alter column "%s" set data type varchar', $columnName)
        ]);
        $tests['Change a column data type to a synonymous type'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        //Increase the length of a text/string column (e.g. VARCHAR(50) to VARCHAR(100))
        $currentColumn = new Column();
        $currentColumn->setName($columnName);
        $currentColumn->setType('varchar');
        $currentColumn->setLimit(50);
        $newColumn = new Column();
        $newColumn->setName($columnName);
        $newColumn->setType('varchar');
        $newColumn->setLimit(100);
        $instructions = new AlterInstructions([
            sprintf('alter column "%s" set data type varchar(100)', $columnName)
        ]);
        $tests['Increase the length of a text/string column'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        $newColumn = clone $newColumn;
        $newColumn->setType('string');
        $tests['Increase the length of a varchar column defined by string synonym'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        //Increase the length of a text/string column with collation
        $currentColumn = new Column();
        $currentColumn->setName($columnName);
        $currentColumn->setType('varchar');
        $currentColumn->setLimit(50);
        $currentColumn->setCollation('en-cs');
        $newColumn = new Column();
        $newColumn->setName($columnName);
        $newColumn->setType('varchar');
        $newColumn->setLimit(100);
        $instructions = new AlterInstructions([
            sprintf('alter column "%s" set data type varchar(100) collate \'en-cs\'', $columnName)
        ]);
        $tests['Increase the length of a text/string column with collation'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        //Increase the precision of a number column (e.g. NUMBER(10,2) to NUMBER(20,2))
        $currentColumn = new Column();
        $currentColumn->setName($columnName);
        $currentColumn->setType('number');
        $currentColumn->setPrecision(10);
        $currentColumn->setScale(2);
        $newColumn = new Column();
        $newColumn->setName($columnName);
        $newColumn->setType('number');
        $newColumn->setPrecision(20);
        $instructions = new AlterInstructions([
            sprintf('alter "%s" set data type number(20,2)', $columnName)
        ]);
        $tests['Increase the precision of a number column'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        //Decrease the precision of a number column (e.g. NUMBER(20,2) to NUMBER(10,2))
        $currentColumn = new Column();
        $currentColumn->setName($columnName);
        $currentColumn->setType('number');
        $currentColumn->setPrecision(20);
        $currentColumn->setScale(2);
        $newColumn = new Column();
        $newColumn->setName($columnName);
        $newColumn->setType('number');
        $newColumn->setPrecision(10);
        $instructions = new AlterInstructions([
            sprintf('alter "%s" set data type number(10,2)', $columnName)
        ]);
        $tests['Decrease the precision of a number column'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        //Unset the comment for a column
        $currentColumn = new Column();
        $currentColumn->setType('varchar');
        $currentColumn->setName($columnName);
        $currentColumn->setComment('Lorem Ipsum');
        $newColumn = new Column();
        $newColumn->setType('varchar');
        $newColumn->setName($columnName);
        $newColumn->setComment(null);
        $instructions = new AlterInstructions([
            sprintf('alter "%s" unset comment', $columnName)
        ]);
        $tests['Unset the comment for a column'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        //Set the comment for a column
        $currentColumn = new Column();
        $currentColumn->setType('varchar');
        $currentColumn->setName($columnName);
        $currentColumn->setComment('Lorem Ipsum');
        $newColumn = new Column();
        $newColumn->setType('varchar');
        $newColumn->setName($columnName);
        $newColumn->setComment('Ipsum Lorem');
        $instructions = new AlterInstructions([
            sprintf('alter "%s" comment \'Ipsum Lorem\'', $columnName)
        ]);
        $tests['Set the comment for a column'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        //Nullable to not nullable and increase the length
        $currentColumn = new Column();
        $currentColumn->setName($columnName);
        $currentColumn->setType('varchar');
        $currentColumn->setNull(true);
        $currentColumn->setLimit(42);
        $newColumn = new Column();
        $newColumn->setName($columnName);
        $newColumn->setType('varchar');
        $newColumn->setNull(false);
        $newColumn->setLimit(4242);
        $instructions = new AlterInstructions([
            sprintf('alter column "%s" set not null', $columnName),
            sprintf('alter column "%s" set data type varchar(4242)', $columnName),
        ]);
        $tests['Nullable to not nullable and increase the length'] = [
            'newColumn' => $newColumn,
            'columns' => [$currentColumn],
            'instructions' => $instructions,
        ];

        //TODO: Set or unset a Column-level Security masking policy on a column.

        //TODO: Set or unset a tag on a column

/*        foreach ($tests as $name => $test) {
            if ($name != 'Increase the length of a varchar column defined by string synonym') {
                unset($tests[$name]);
            }
        }*/
        return $tests;

    }

    /**
     * @dataProvider bulkInsertDataProvider
     * @throws Exception
     */
    public function testBulkInsert(array $rows, array $expected)
    {
        $input = $this->createStub(Input::class);
        $output = $this->createStub(Output::class);

        $pdo = $this->createPartialMock(PDO::class, ['exec']);
        $pdo->expects($this->once())
            ->method('exec')
            ->with($expected['sql'])
            ->willReturn(count($rows));

        $adapter = $this->getMockBuilder(SnowflakeAdapter::class)
            ->setConstructorArgs([[], $input, $output])
            ->onlyMethods(['getConnection', 'isDryRunEnabled'])
            ->getMock();
        $adapter->expects($this->once())
            ->method('getConnection')
            ->willReturn($pdo);
        $adapter->expects($this->once())
            ->method('isDryRunEnabled')
            ->willReturn(false);

        $table = new Table('table');

        $adapter->bulkinsert($table, $rows);
    }

    /**
     * @dataProvider bulkInsertDataProvider
     * @throws Exception
     */
    public function testBulkInsertWithDryRunEnabled(array $rows, array $expected)
    {
        $input = $this->createStub(InputInterface::class);
        $output = $this->createStub(OutputInterface::class);
        $output->expects($this->once())
            ->method('writeln')
            ->with($expected['sql']);

        $adapter = $this->getMockBuilder(SnowflakeAdapter::class)
            ->setConstructorArgs([[], $input, $output])
            ->onlyMethods(['isDryRunEnabled', 'getConnection'])
            ->getMock();
        $adapter->expects($this->atLeastOnce())
            ->method('isDryRunEnabled')
            ->willReturn(true);

        $table = new Table('table');

        $adapter->bulkinsert($table, $rows);
    }

    public static function bulkInsertDataProvider(): array
    {
        return [
            'simple fields and values' => [
                'rows' => [
                    ['column1' => "'value1 row0", 'column2' => 'value2 row0', 'column3' => 'value3 row0'],
                    ['column1' => 'value1 row1', 'column2' => 'value2 row1', 'column3' => 'value3 row1'],
                    ['column1' => 'value1 row2', 'column2' => 'value2 row2', 'column3' => 'value3 row2'],
                    ['column1' => 'value1 row3', 'column2' => 'value2 row3', 'column3' => 'value3 row3'],
                    ['column1' => 'value1 row4', 'column2' => 'value2 row4', 'column3' => 'value3 row4'],
                ],
                'expected' => [
                    'preparedStatementWithNamedParameters' => 'insert into "table" ("column1","column2","column3") values ' .
                        '(:0column1,:0column2,:0column3),' .
                        '(:1column1,:1column2,:1column3),' .
                        '(:2column1,:2column2,:2column3),' .
                        '(:3column1,:3column2,:3column3),' .
                        '(:4column1,:4column2,:4column3)',
                    'preparedStatementWithQuestionMarkParameters' => 'insert into "table" ("column1","column2","column3") values ' .
                        '(?,?,?),' .
                        '(?,?,?),' .
                        '(?,?,?),' .
                        '(?,?,?),' .
                        '(?,?,?)',
                    'sql' => 'insert into "table" ("column1","column2","column3") values ' .
                        "('\'value1 row0','value2 row0','value3 row0')," .
                        "('value1 row1','value2 row1','value3 row1')," .
                        "('value1 row2','value2 row2','value3 row2')," .
                        "('value1 row3','value2 row3','value3 row3')," .
                        "('value1 row4','value2 row4','value3 row4')",
                    'values' => [
                        ':0column1' => "'value1 row0", ':0column2' => 'value2 row0', ':0column3' => 'value3 row0',
                        ':1column1' => 'value1 row1', ':1column2' => 'value2 row1', ':1column3' => 'value3 row1',
                        ':2column1' => 'value1 row2', ':2column2' => 'value2 row2', ':2column3' => 'value3 row2',
                        ':3column1' => 'value1 row3', ':3column2' => 'value2 row3', ':3column3' => 'value3 row3',
                        ':4column1' => 'value1 row4', ':4column2' => 'value2 row4', ':4column3' => 'value3 row4',
                    ]
                ],
            ]
        ];
    }

    /**
     * @dataProvider quoteValueDataProvider
     */
    public function testQuoteValue($value, $expected)
    {
        $adapter = new SnowflakeAdapter([]);
        if ($expected != 'exception') {
            $this->assertEquals($expected, $adapter->quoteValue($value));
            $this->assertEquals(gettype($expected), getType($adapter->quoteValue($value)));
        } else {
            $this->expectException(\Throwable::class);
            $adapter->quoteValue($value);
        }
    }

    public static function quoteValueDataProvider(): array
    {
        return [
            'integer' => [
                'value' => 42,
                'expected' => 42,
            ],
            'float' => [
                'value' => 42.42,
                'expected' => 42.42,
            ],
            'string' => [
                'value' => 'lorem ipsum',
                'expected' => "'lorem ipsum'",
            ],
            'null' => [
                'value' => null,
                'expected' => 'null',
            ],
            'boolean true' => [
                'value' => true,
                'expected' => 'true',
            ],
            'boolean false' => [
                'value' => false,
                'expected' => 'false',
            ],
            'literal' => [
                'value' => new Literal('lorem ipsum'),
                'expected' => 'lorem ipsum',
            ],
            'non stringable object' => [
                'value' => new \stdClass(),
                'expected' => 'exception',
            ],
        ];
    }

    /**
     * @dataProvider getDataTypeBySynonymDataProvider
     */
    public function testGetDataTypeBySynonym(string $type, string $expected)
    {
        $adapter = new SnowflakeAdapter([]);
        if ('exception' != $expected) {
            $this->assertEquals($expected, $adapter->getDataTypeBySynonym($type));
        } else {
            $this->expectException(InvalidArgumentException::class);
            $adapter->getDataTypeBySynonym($type);
        }
    }

    public static function getDataTypeBySynonymDataProvider(): array
    {
        $types = [
            'number' => [
                'number', 'decimal', 'numeric', 'int', 'integer', 'bigint', 'smallint', 'tinyint', 'byteint',
            ],
            'float' => [
                'float', 'float4', 'float8', 'double', 'double precision', 'real',
            ],
            'varchar' => [
                'varchar', 'char', 'character', 'nchar', 'string', 'text', 'nvarchar', 'nvarchar2', 'char varying', 'nchar varying',
            ],
            'boolean' => [
                'boolean',
            ],
            'date' => [
                'date',
            ],
            'time' => [
                'time',
            ],
            'timestamp_ntz' => [
                'timestamp_ntz', 'datetime', 'timestampntz', 'timestamp without time zone',
            ],
            'timestamp_ltz' => [
                'timestamp_ltz', 'timestampltz', 'timestamp with local time zone',
            ],
            'timestamp_tz' => [
                'timestamp_tz', 'timestamptz', 'timestamp with time zone',
            ],
            'variant' => [
                'variant',
            ],
            'array' => [
                'array',
            ],
            'object' => [
                'object',
            ],
            'geography' => [
                'geography',
            ],
            'geometry' => [
                'geometry',
            ],
            'exception' => [
                'unknown data type'
            ],
        ];
        $data = [];
        foreach ($types as $realType => $synonymous) {
            foreach ($synonymous as $synonym) {
                $data["$synonym=$realType"] = [$synonym, $realType];
            }
        }
        return $data;
    }

    /**
     * @dataProvider getForeignKeysDataProvider
     * @throws Exception
     */
    public function testGetForeignKeys(string $tableName, string $expectedSql, array $fetchAllWillReturn, array $expectedForeignKeys)
    {
        $adapter = $this->createPartialMock(SnowflakeAdapter::class, ['fetchAll']);
        $adapter->expects($this->once())
            ->method('fetchAll')
            ->with($expectedSql)
            ->willReturn($fetchAllWillReturn);
        $foreignKeys = $adapter->getForeignKeys($tableName);
        $this->assertEquals($expectedForeignKeys, $foreignKeys);
    }

    public static function getForeignKeysDataProvider(): array
    {
        $withoutTableNameFetchAllWillReturn = array(
            0 =>
                array(
                    'created_on' => '2023-04-02 04:17:55.536',
                    0 => '2023-04-02 04:17:55.536',
                    'pk_database_name' => 'TEST_DATABASE',
                    1 => 'TEST_DATABASE',
                    'pk_schema_name' => 'TEST_SCHEMA',
                    2 => 'TEST_SCHEMA',
                    'pk_table_name' => 'multiuniqueparent',
                    3 => 'multiuniqueparent',
                    'pk_column_name' => 'id1',
                    4 => 'id1',
                    'fk_database_name' => 'TEST_DATABASE',
                    5 => 'TEST_DATABASE',
                    'fk_schema_name' => 'TEST_SCHEMA',
                    6 => 'TEST_SCHEMA',
                    'fk_table_name' => 'multiuniquechild',
                    7 => 'multiuniquechild',
                    'fk_column_name' => 'parent_id1',
                    8 => 'parent_id1',
                    'key_sequence' => '1',
                    9 => '1',
                    'update_rule' => 'NO ACTION',
                    10 => 'NO ACTION',
                    'delete_rule' => 'NO ACTION',
                    11 => 'NO ACTION',
                    'fk_name' => 'parent_child_foreign_key_constraint',
                    12 => 'parent_child_foreign_key_constraint',
                    'pk_name' => 'MULTIUNIQUEPARENT_ID1_AND_ID2',
                    13 => 'MULTIUNIQUEPARENT_ID1_AND_ID2',
                    'deferrability' => 'NOT DEFERRABLE',
                    14 => 'NOT DEFERRABLE',
                    'rely' => 'false',
                    15 => 'false',
                    'comment' => NULL,
                    16 => NULL,
                ),
            1 =>
                array(
                    'created_on' => '2023-04-02 04:17:55.536',
                    0 => '2023-04-02 04:17:55.536',
                    'pk_database_name' => 'TEST_DATABASE',
                    1 => 'TEST_DATABASE',
                    'pk_schema_name' => 'TEST_SCHEMA',
                    2 => 'TEST_SCHEMA',
                    'pk_table_name' => 'multiuniqueparent',
                    3 => 'multiuniqueparent',
                    'pk_column_name' => 'id2',
                    4 => 'id2',
                    'fk_database_name' => 'TEST_DATABASE',
                    5 => 'TEST_DATABASE',
                    'fk_schema_name' => 'TEST_SCHEMA',
                    6 => 'TEST_SCHEMA',
                    'fk_table_name' => 'multiuniquechild',
                    7 => 'multiuniquechild',
                    'fk_column_name' => 'parent_id2',
                    8 => 'parent_id2',
                    'key_sequence' => '2',
                    9 => '2',
                    'update_rule' => 'NO ACTION',
                    10 => 'NO ACTION',
                    'delete_rule' => 'NO ACTION',
                    11 => 'NO ACTION',
                    'fk_name' => 'parent_child_foreign_key_constraint',
                    12 => 'parent_child_foreign_key_constraint',
                    'pk_name' => 'MULTIUNIQUEPARENT_ID1_AND_ID2',
                    13 => 'MULTIUNIQUEPARENT_ID1_AND_ID2',
                    'deferrability' => 'NOT DEFERRABLE',
                    14 => 'NOT DEFERRABLE',
                    'rely' => 'false',
                    15 => 'false',
                    'comment' => NULL,
                    16 => NULL,
                ),
            2 =>
                array(
                    'created_on' => '2023-04-04 02:46:47.851',
                    0 => '2023-04-04 02:46:47.851',
                    'pk_database_name' => 'TEST_DATABASE',
                    1 => 'TEST_DATABASE',
                    'pk_schema_name' => 'TEST_SCHEMA',
                    2 => 'TEST_SCHEMA',
                    'pk_table_name' => 'users',
                    3 => 'users',
                    'pk_column_name' => 'id',
                    4 => 'id',
                    'fk_database_name' => 'TEST_DATABASE',
                    5 => 'TEST_DATABASE',
                    'fk_schema_name' => 'TEST_SCHEMA',
                    6 => 'TEST_SCHEMA',
                    'fk_table_name' => 'articles',
                    7 => 'articles',
                    'fk_column_name' => 'author_id',
                    8 => 'author_id',
                    'key_sequence' => '1',
                    9 => '1',
                    'update_rule' => 'NO ACTION',
                    10 => 'NO ACTION',
                    'delete_rule' => 'NO ACTION',
                    11 => 'NO ACTION',
                    'fk_name' => 'SYS_CONSTRAINT_937e6ff8-840a-456a-853a-51770ce0849b',
                    12 => 'SYS_CONSTRAINT_937e6ff8-840a-456a-853a-51770ce0849b',
                    'pk_name' => 'SYS_CONSTRAINT_0b150baf-4768-4152-87cb-ca8d8f7240cf',
                    13 => 'SYS_CONSTRAINT_0b150baf-4768-4152-87cb-ca8d8f7240cf',
                    'deferrability' => 'NOT DEFERRABLE',
                    14 => 'NOT DEFERRABLE',
                    'rely' => 'false',
                    15 => 'false',
                    'comment' => NULL,
                    16 => NULL,
                ),
            3 =>
                array(
                    'created_on' => '2023-04-04 02:46:48.160',
                    0 => '2023-04-04 02:46:48.160',
                    'pk_database_name' => 'TEST_DATABASE',
                    1 => 'TEST_DATABASE',
                    'pk_schema_name' => 'TEST_SCHEMA',
                    2 => 'TEST_SCHEMA',
                    'pk_table_name' => 'users',
                    3 => 'users',
                    'pk_column_name' => 'id',
                    4 => 'id',
                    'fk_database_name' => 'TEST_DATABASE',
                    5 => 'TEST_DATABASE',
                    'fk_schema_name' => 'TEST_SCHEMA',
                    6 => 'TEST_SCHEMA',
                    'fk_table_name' => 'articles',
                    7 => 'articles',
                    'fk_column_name' => 'reviewer_id',
                    8 => 'reviewer_id',
                    'key_sequence' => '1',
                    9 => '1',
                    'update_rule' => 'NO ACTION',
                    10 => 'NO ACTION',
                    'delete_rule' => 'NO ACTION',
                    11 => 'NO ACTION',
                    'fk_name' => 'LOREM_IPSUM',
                    12 => 'LOREM_IPSUM',
                    'pk_name' => 'SYS_CONSTRAINT_0b150baf-4768-4152-87cb-ca8d8f7240cf',
                    13 => 'SYS_CONSTRAINT_0b150baf-4768-4152-87cb-ca8d8f7240cf',
                    'deferrability' => 'NOT DEFERRABLE',
                    14 => 'NOT DEFERRABLE',
                    'rely' => 'false',
                    15 => 'false',
                    'comment' => NULL,
                    16 => NULL,
                ),
            4 =>
                array(
                    'created_on' => '2023-04-04 05:16:22.883',
                    0 => '2023-04-04 05:16:22.883',
                    'pk_database_name' => 'TEST_DATABASE',
                    1 => 'TEST_DATABASE',
                    'pk_schema_name' => 'TEST_SCHEMA',
                    2 => 'TEST_SCHEMA',
                    'pk_table_name' => 'users',
                    3 => 'users',
                    'pk_column_name' => 'other_id1',
                    4 => 'other_id1',
                    'fk_database_name' => 'TEST_DATABASE',
                    5 => 'TEST_DATABASE',
                    'fk_schema_name' => 'TEST_SCHEMA',
                    6 => 'TEST_SCHEMA',
                    'fk_table_name' => 'blog',
                    7 => 'blog',
                    'fk_column_name' => 'user1_id',
                    8 => 'user1_id',
                    'key_sequence' => '1',
                    9 => '1',
                    'update_rule' => 'NO ACTION',
                    10 => 'NO ACTION',
                    'delete_rule' => 'NO ACTION',
                    11 => 'NO ACTION',
                    'fk_name' => 'main_table_blog_referenced_table_user_constraint',
                    12 => 'main_table_blog_referenced_table_user_constraint',
                    'pk_name' => 'SYS_CONSTRAINT_2fff8d13-7f5c-4d6c-9968-d934af4df010',
                    13 => 'SYS_CONSTRAINT_2fff8d13-7f5c-4d6c-9968-d934af4df010',
                    'deferrability' => 'NOT DEFERRABLE',
                    14 => 'NOT DEFERRABLE',
                    'rely' => 'false',
                    15 => 'false',
                    'comment' => NULL,
                    16 => NULL,
                ),
            5 =>
                array(
                    'created_on' => '2023-04-04 02:46:47.562',
                    0 => '2023-04-04 02:46:47.562',
                    'pk_database_name' => 'TEST_DATABASE',
                    1 => 'TEST_DATABASE',
                    'pk_schema_name' => 'TEST_SCHEMA',
                    2 => 'TEST_SCHEMA',
                    'pk_table_name' => 'users',
                    3 => 'users',
                    'pk_column_name' => 'other_id1',
                    4 => 'other_id1',
                    'fk_database_name' => 'TEST_DATABASE',
                    5 => 'TEST_DATABASE',
                    'fk_schema_name' => 'TEST_SCHEMA',
                    6 => 'TEST_SCHEMA',
                    'fk_table_name' => 'articles',
                    7 => 'articles',
                    'fk_column_name' => 'author_id',
                    8 => 'author_id',
                    'key_sequence' => '1',
                    9 => '1',
                    'update_rule' => 'NO ACTION',
                    10 => 'NO ACTION',
                    'delete_rule' => 'NO ACTION',
                    11 => 'NO ACTION',
                    'fk_name' => 'SYS_CONSTRAINT_358724a8-e027-4b3e-8ab5-176852a7bd37',
                    12 => 'SYS_CONSTRAINT_358724a8-e027-4b3e-8ab5-176852a7bd37',
                    'pk_name' => 'SYS_CONSTRAINT_2fff8d13-7f5c-4d6c-9968-d934af4df010',
                    13 => 'SYS_CONSTRAINT_2fff8d13-7f5c-4d6c-9968-d934af4df010',
                    'deferrability' => 'NOT DEFERRABLE',
                    14 => 'NOT DEFERRABLE',
                    'rely' => 'false',
                    15 => 'false',
                    'comment' => NULL,
                    16 => NULL,
                ),
            6 =>
                array(
                    'created_on' => '2023-04-04 05:16:22.883',
                    0 => '2023-04-04 05:16:22.883',
                    'pk_database_name' => 'TEST_DATABASE',
                    1 => 'TEST_DATABASE',
                    'pk_schema_name' => 'TEST_SCHEMA',
                    2 => 'TEST_SCHEMA',
                    'pk_table_name' => 'users',
                    3 => 'users',
                    'pk_column_name' => 'other_id2',
                    4 => 'other_id2',
                    'fk_database_name' => 'TEST_DATABASE',
                    5 => 'TEST_DATABASE',
                    'fk_schema_name' => 'TEST_SCHEMA',
                    6 => 'TEST_SCHEMA',
                    'fk_table_name' => 'blog',
                    7 => 'blog',
                    'fk_column_name' => 'user2_id',
                    8 => 'user2_id',
                    'key_sequence' => '2',
                    9 => '2',
                    'update_rule' => 'NO ACTION',
                    10 => 'NO ACTION',
                    'delete_rule' => 'NO ACTION',
                    11 => 'NO ACTION',
                    'fk_name' => 'main_table_blog_referenced_table_user_constraint',
                    12 => 'main_table_blog_referenced_table_user_constraint',
                    'pk_name' => 'SYS_CONSTRAINT_2fff8d13-7f5c-4d6c-9968-d934af4df010',
                    13 => 'SYS_CONSTRAINT_2fff8d13-7f5c-4d6c-9968-d934af4df010',
                    'deferrability' => 'NOT DEFERRABLE',
                    14 => 'NOT DEFERRABLE',
                    'rely' => 'false',
                    15 => 'false',
                    'comment' => NULL,
                    16 => NULL,
                ),
            7 =>
                array(
                    'created_on' => '2023-04-04 02:46:47.562',
                    0 => '2023-04-04 02:46:47.562',
                    'pk_database_name' => 'TEST_DATABASE',
                    1 => 'TEST_DATABASE',
                    'pk_schema_name' => 'TEST_SCHEMA',
                    2 => 'TEST_SCHEMA',
                    'pk_table_name' => 'users',
                    3 => 'users',
                    'pk_column_name' => 'other_id2',
                    4 => 'other_id2',
                    'fk_database_name' => 'TEST_DATABASE',
                    5 => 'TEST_DATABASE',
                    'fk_schema_name' => 'TEST_SCHEMA',
                    6 => 'TEST_SCHEMA',
                    'fk_table_name' => 'articles',
                    7 => 'articles',
                    'fk_column_name' => 'reviewer_id',
                    8 => 'reviewer_id',
                    'key_sequence' => '2',
                    9 => '2',
                    'update_rule' => 'NO ACTION',
                    10 => 'NO ACTION',
                    'delete_rule' => 'NO ACTION',
                    11 => 'NO ACTION',
                    'fk_name' => 'SYS_CONSTRAINT_358724a8-e027-4b3e-8ab5-176852a7bd37',
                    12 => 'SYS_CONSTRAINT_358724a8-e027-4b3e-8ab5-176852a7bd37',
                    'pk_name' => 'SYS_CONSTRAINT_2fff8d13-7f5c-4d6c-9968-d934af4df010',
                    13 => 'SYS_CONSTRAINT_2fff8d13-7f5c-4d6c-9968-d934af4df010',
                    'deferrability' => 'NOT DEFERRABLE',
                    14 => 'NOT DEFERRABLE',
                    'rely' => 'false',
                    15 => 'false',
                    'comment' => NULL,
                    16 => NULL,
                ),
        );
        $tableNameBlogFetchAllWillReturn = [$withoutTableNameFetchAllWillReturn[4], $withoutTableNameFetchAllWillReturn[6]];
        return [
            'table name is specified' => [
                'tableName' => 'blog',
                'expectedSql' => 'show imported keys in table "blog"',
                'fetchAllWillReturn' => $tableNameBlogFetchAllWillReturn,
                'expectedForeignKeys' => array(
                    'main_table_blog_referenced_table_user_constraint' =>
                        array(
                            'table' => 'TEST_DATABASE.TEST_SCHEMA.blog',
                            'columns' =>
                                array(
                                    0 => 'user1_id',
                                    1 => 'user2_id',
                                ),
                            'referenced_table' => 'TEST_DATABASE.TEST_SCHEMA.users',
                            'referenced_columns' =>
                                array(
                                    0 => 'other_id1',
                                    1 => 'other_id2',
                                ),
                        ),
                )
            ],
            'table name is not specified' => [
                'tableName' => '',
                'expectedSql' => 'show imported keys',
                'fetchAllWillReturn' => $withoutTableNameFetchAllWillReturn,
                'expectedForeignKeys' => array(
                    'parent_child_foreign_key_constraint' =>
                        array(
                            'table' => 'TEST_DATABASE.TEST_SCHEMA.multiuniquechild',
                            'columns' =>
                                array(
                                    0 => 'parent_id1',
                                    1 => 'parent_id2',
                                ),
                            'referenced_table' => 'TEST_DATABASE.TEST_SCHEMA.multiuniqueparent',
                            'referenced_columns' =>
                                array(
                                    0 => 'id1',
                                    1 => 'id2',
                                ),
                        ),
                    'SYS_CONSTRAINT_937e6ff8-840a-456a-853a-51770ce0849b' =>
                        array(
                            'table' => 'TEST_DATABASE.TEST_SCHEMA.articles',
                            'columns' =>
                                array(
                                    0 => 'author_id',
                                ),
                            'referenced_table' => 'TEST_DATABASE.TEST_SCHEMA.users',
                            'referenced_columns' =>
                                array(
                                    0 => 'id',
                                ),
                        ),
                    'LOREM_IPSUM' =>
                        array(
                            'table' => 'TEST_DATABASE.TEST_SCHEMA.articles',
                            'columns' =>
                                array(
                                    0 => 'reviewer_id',
                                ),
                            'referenced_table' => 'TEST_DATABASE.TEST_SCHEMA.users',
                            'referenced_columns' =>
                                array(
                                    0 => 'id',
                                ),
                        ),
                    'main_table_blog_referenced_table_user_constraint' =>
                        array(
                            'table' => 'TEST_DATABASE.TEST_SCHEMA.blog',
                            'columns' =>
                                array(
                                    0 => 'user1_id',
                                    1 => 'user2_id',
                                ),
                            'referenced_table' => 'TEST_DATABASE.TEST_SCHEMA.users',
                            'referenced_columns' =>
                                array(
                                    0 => 'other_id1',
                                    1 => 'other_id2',
                                ),
                        ),
                    'SYS_CONSTRAINT_358724a8-e027-4b3e-8ab5-176852a7bd37' =>
                        array(
                            'table' => 'TEST_DATABASE.TEST_SCHEMA.articles',
                            'columns' =>
                                array(
                                    0 => 'author_id',
                                    1 => 'reviewer_id',
                                ),
                            'referenced_table' => 'TEST_DATABASE.TEST_SCHEMA.users',
                            'referenced_columns' =>
                                array(
                                    0 => 'other_id1',
                                    1 => 'other_id2',
                                ),
                        ),
                )
            ],
        ];
    }

    /**
     * @dataProvider getAddIndexInstructionsDataProvider
     * @throws ReflectionException
     */
    public function testGetAddIndexInstructions(Table $table, Index $index, string $expected)
    {
        $adapter = new SnowflakeAdapter([]);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getAddIndexInstructions');
        if ('exception' != $expected) {
            $addIndexInstructions = $method->invoke($adapter, $table, $index);
            $this->assertInstanceOf(AlterInstructions::class, $addIndexInstructions);
            $this->assertCount(0, $addIndexInstructions->getPostSteps());
            $this->assertEquals($expected, $addIndexInstructions->getAlterParts()[0]);
        } else {
            $this->expectException(\Throwable::class);
            $method->invoke($adapter, $table, $index);
        }
    }

    public static function getAddIndexInstructionsDataProvider(): array
    {
        $table = new Table('table');

        $uniqueIndexOneColumn = new Index();
        $uniqueIndexOneColumn->setType('unique');
        $uniqueIndexOneColumn->setColumns('column');

        $uniqueIndexTwoColumn = new Index();
        $uniqueIndexTwoColumn->setType('unique');
        $uniqueIndexTwoColumn->setColumns(['column1', 'column2']);

        $uniqueIndexOneColumnNamed = clone $uniqueIndexOneColumn;
        $uniqueIndexOneColumnNamed->setName('name');

        $uniqueIndexTwoColumnNamed = clone $uniqueIndexTwoColumn;
        $uniqueIndexTwoColumnNamed->setName('name');

        $notUniqueIndex = new Index();
        $notUniqueIndex->setType('fulltext');

        return [
            'Unique index type, one column' => [
                'table' => $table,
                'index' => $uniqueIndexOneColumn,
                'expected' => 'alter table "table" add constraint unique ("column")',
            ],
            'Unique index type, two column' => [
                'table' => $table,
                'index' => $uniqueIndexTwoColumn,
                'expected' => 'alter table "table" add constraint unique ("column1","column2")',
            ],
            'Unique index type, one column, named' => [
                'table' => $table,
                'index' => $uniqueIndexOneColumnNamed,
                'expected' => 'alter table "table" add constraint "name" unique ("column")',
            ],
            'Unique index type, two column, named' => [
                'table' => $table,
                'index' => $uniqueIndexTwoColumnNamed,
                'expected' => 'alter table "table" add constraint "name" unique ("column1","column2")',
            ],
            'Index type is not unique' => [
                'table' => $table,
                'index' => $notUniqueIndex,
                'expected' => 'exception'
            ]
        ];
    }

    /**
     * @dataProvider hasForeignKeyDataProvider
     * @throws Exception
     */
    public function testHasForeignKey(string $tableName, array|string|null $columns, string|null $constraint, bool $expected)
    {
        $adapter = $this->createPartialMock(SnowflakeAdapter::class, ['fetchAll']);
        $adapter->expects($this->once())
            ->method('fetchAll')
            ->willReturn(self::getForeignKeysDataProvider()['table name is not specified']['fetchAllWillReturn']);
        $this->assertEquals($expected, $adapter->hasForeignKey($tableName, $columns, $constraint));
    }

    public static function hasForeignKeyDataProvider(): array
    {
        return [
            'two columns' => [
                'tableName' => 'blog',
                'columns' => ['user1_id', 'user2_id'],
                'constraint' => '',
                'expected' => true,
            ],
            'one column' => [
                'tableName' => 'articles',
                'columns' => 'author_id',
                'constraint' => '',
                'expected' => true,
            ],
            'existing constraint' => [
                'tableName' => 'blog',
                'columns' => null,
                'constraint' => 'main_table_blog_referenced_table_user_constraint',
                'expected' => true,
            ],
            'non-existing constraint' => [
                'tableName' => 'blog',
                'columns' => null,
                'constraint' => 'non-existing constraint',
                'expected' => false,
            ],
            'nothing' => [
                'tableName' => '',
                'columns' => null,
                'constraint' => null,
                'expected' => false,
            ],
        ];
    }

    /**
     * @throws ReflectionException
     */
    public function testGetDropForeignKeyInstructions()
    {
        $tableName = 'table';
        $constraint = 'lorem ipsum dolor sit amet';
        $expected = 'drop constraint "lorem ipsum dolor sit amet"';
        $adapter = new SnowflakeAdapter([]);
        $reflection = new ReflectionObject($adapter);
        $method = $reflection->getMethod('getDropForeignKeyInstructions');
        $instructions = $method->invoke($adapter, $tableName, $constraint);
        $this->assertEquals($expected, $instructions->getAlterParts()[0]);
    }

}
