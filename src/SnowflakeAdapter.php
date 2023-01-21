<?php
declare(strict_types=1);

namespace Szabacsik\Phinx;

use Cake\Database\Connection;
use Phinx\Db\Adapter\PdoAdapter;
use Phinx\Db\Table\Column;
use Phinx\Db\Table\ForeignKey;
use Phinx\Db\Table\Index;
use Phinx\Db\Table\Table;
use Phinx\Db\Util\AlterInstructions;
use Phinx\Config\Config;
use RuntimeException;
use PDOException;

class SnowflakeAdapter extends PdoAdapter
{
    public function hasTransactions(): bool
    {
        return true;
    }

    public function beginTransaction(): void
    {
        $this->execute('begin transaction');
    }

    public function commitTransaction(): void
    {
        $this->execute('commit');
    }

    public function rollbackTransaction(): void
    {
        $this->execute('rollback');
    }

    /**
     * Snowflake uses case-insensitive comparison for unquoted identifiers such as table and column names by default.
     * This means that when querying a table, you can use uppercase or lowercase letters for the table name
     * and Snowflake will still be able to match it. However, if you want to
     * force Snowflake to differentiate between uppercase and lowercase characters in table names,
     * you can use double quotes around the table name when creating it.
     * @see https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
     */
    public function quoteTableName(string $tableName): string
    {
        return '"' . $tableName . '"';
    }

    /**
     * Snowflake stores database objects (such as tables and columns) in uppercase by default.
     * This means that even if you create a table or column with lowercase letters,
     * Snowflake will still return the names in uppercase when queried.
     * This can cause issues if your code is expecting the names to be returned in lowercase.
     * To prevent this issue, this function uses double quotes around the column names in the SQL query.
     * This forces Snowflake to treat these identifiers as case-sensitive and returns the names as they were created.
     * Using double quotes around the table and column names is
     * a best practice to ensure that the data is queried correctly.
     * @see https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
     */
    public function quoteColumnName(string $columnName): string
    {
        return '"' . $columnName . '"';
    }

    public function hasTable(string $tableName): bool
    {
        $sql = "show tables like '$tableName'";
        return (bool)$this->fetchRow($sql);
    }

    public function createTable(Table $table, array $columns = [], array $indexes = []): void
    {
        $separator = ', ';
        $sql = "create table {$this->quoteTableName($table->getName())} (";
        foreach ($columns as $column) {
            $sql .= $this->quoteColumnName($column->getName()) . ' ' . $this->getColumnSqlDefinition($column) . $separator;
        }

        if (isset($table->getOptions()['primary_key'])) {
            $sql .= 'primary key ("';
            $sql .= (
                is_array($table->getOptions()['primary_key'])
                    ? implode('", "', $table->getOptions()['primary_key'])
                    : $table->getOptions()['primary_key']
                ) . '")';
        }

        $sql = rtrim($sql, $separator);
        $sql .= ')';

        $this->execute($sql);
    }

    public function truncateTable(string $tableName): void
    {
        // TODO: Implement truncateTable() method.
    }

    public function getColumns(string $tableName): array
    {
        // TODO: Implement getColumns() method.
    }

    public function hasColumn(string $tableName, string $columnName): bool
    {
        $sql = "show columns like '$columnName' in table {$this->quoteTableName($tableName)}";
        return (bool)$this->query($sql)->fetch();
    }

    public function hasIndex(string $tableName, $columns): bool
    {
        // TODO: Implement hasIndex() method.
    }

    public function hasIndexByName(string $tableName, string $indexName): bool
    {
        // TODO: Implement hasIndexByName() method.
    }

    public function hasPrimaryKey(string $tableName, $columns, ?string $constraint = null): bool
    {
        // TODO: Implement hasPrimaryKey() method.
    }

    public function hasForeignKey(string $tableName, $columns, ?string $constraint = null): bool
    {
        // TODO: Implement hasForeignKey() method.
    }

    public function getSqlType($type, ?int $limit = null): array
    {
        // TODO: Implement getSqlType() method.
    }

    public function createDatabase(string $name, array $options = []): void
    {
        // TODO: Implement createDatabase() method.
    }

    public function hasDatabase(string $name): bool
    {
        // TODO: Implement hasDatabase() method.
    }

    public function dropDatabase(string $name): void
    {
        // TODO: Implement dropDatabase() method.
    }

    public function connect(): void
    {
        // TODO: Implement connect() method.
    }

    public function disconnect(): void
    {
        // TODO: Implement disconnect() method.
    }

    public function getDecoratedConnection(): Connection
    {
        // TODO: Implement getDecoratedConnection() method.
    }

    protected function getAddColumnInstructions(Table $table, Column $column): AlterInstructions
    {
        // TODO: Implement getAddColumnInstructions() method.
    }

    protected function getRenameColumnInstructions(string $tableName, string $columnName, string $newColumnName): AlterInstructions
    {
        // TODO: Implement getRenameColumnInstructions() method.
    }

    protected function getChangeColumnInstructions(string $tableName, string $columnName, Column $newColumn): AlterInstructions
    {
        // TODO: Implement getChangeColumnInstructions() method.
    }

    protected function getDropColumnInstructions(string $tableName, string $columnName): AlterInstructions
    {
        // TODO: Implement getDropColumnInstructions() method.
    }

    protected function getAddIndexInstructions(Table $table, Index $index): AlterInstructions
    {
        // TODO: Implement getAddIndexInstructions() method.
    }

    protected function getDropIndexByColumnsInstructions(string $tableName, $columns): AlterInstructions
    {
        // TODO: Implement getDropIndexByColumnsInstructions() method.
    }

    protected function getDropIndexByNameInstructions(string $tableName, string $indexName): AlterInstructions
    {
        // TODO: Implement getDropIndexByNameInstructions() method.
    }

    protected function getAddForeignKeyInstructions(Table $table, ForeignKey $foreignKey): AlterInstructions
    {
        // TODO: Implement getAddForeignKeyInstructions() method.
    }

    protected function getDropForeignKeyInstructions(string $tableName, string $constraint): AlterInstructions
    {
        // TODO: Implement getDropForeignKeyInstructions() method.
    }

    protected function getDropForeignKeyByColumnsInstructions(string $tableName, array $columns): AlterInstructions
    {
        // TODO: Implement getDropForeignKeyByColumnsInstructions() method.
    }

    protected function getDropTableInstructions(string $tableName): AlterInstructions
    {
        // TODO: Implement getDropTableInstructions() method.
    }

    protected function getRenameTableInstructions(string $tableName, string $newTableName): AlterInstructions
    {
        // TODO: Implement getRenameTableInstructions() method.
    }

    protected function getChangePrimaryKeyInstructions(Table $table, $newColumns): AlterInstructions
    {
        // TODO: Implement getChangePrimaryKeyInstructions() method.
    }

    protected function getChangeCommentInstructions(Table $table, ?string $newComment): AlterInstructions
    {
        // TODO: Implement getChangeCommentInstructions() method.
    }

    /**
     * @link https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
     * @link https://docs.snowflake.com/en/sql-reference/data-types-numeric.html
     * @link https://docs.snowflake.com/en/sql-reference/data-types-text.html
     * @link https://docs.snowflake.com/en/sql-reference/data-types-logical.html
     * @link https://docs.snowflake.com/en/sql-reference/data-types-datetime.html
     * @link https://docs.snowflake.com/en/sql-reference/data-types-semistructured.html
     * @link https://docs.snowflake.com/en/sql-reference/data-types-geospatial.html
     * @link https://docs.snowflake.com/en/sql-reference/data-types-unsupported.html
     * @link https://docs.snowflake.com/en/sql-reference/data-type-conversion.html
     * @param Column $column
     * @return string
     */
    public function getColumnSqlDefinition(Column $column): string
    {
        $def = '';

        $synonymousWithNumber = [
            'number', 'decimal', 'numeric'
        ];
        if (in_array($column->getType(), $synonymousWithNumber)) {
            $def = 'number';
            if ($column->getPrecision()) {
                $def .= '(' . $column->getPrecision();
                if ($column->getScale()) {
                    $def .= ",{$column->getScale()})";
                } else {
                    $def .= ',0)';
                }
            }
            if ($column->isIdentity()) {
                $def .= ' identity';
                if ($column->getIncrement() && $column->getSeed()) {
                    $def .= "({$column->getSeed()},{$column->getIncrement()})";
                }
            }
        }

        $synonymousWithNumberWithoutPrecisionAndScale = [
            'int', 'integer', 'bigint', 'smallint', 'tinyint', 'byteint', 'biginteger'
        ];
        if (in_array($column->getType(), $synonymousWithNumberWithoutPrecisionAndScale)) {
            $def = 'number';
        }

        $synonymousWithFloat = ['float', 'float4', 'float8', 'double', 'double precision', 'real'];
        if (in_array($column->getType(), $synonymousWithFloat)) {
            $def = 'float';
        }

        $synonymousWithVarchar = [
            'varchar', 'char', 'character', 'nchar', 'string', 'text',
            'nvarchar', 'nvarchar2', 'char varying', 'nchar varying'
        ];
        if (in_array($column->getType(), $synonymousWithVarchar)) {
            $def = 'varchar';
            if ($column->getLimit()) {
                $def .= "({$column->getLimit()})";
            }
            if ($column->getCollation()) {
                $def .= " collate '{$column->getCollation()}'";
            }
        }

        if ($column->getType() === 'time') {
            $def = 'time';
            if ($column->getPrecision()) {
                $def .= "({$column->getPrecision()})";
            }
        }

        if ($column->getType() === 'date') {
            $def = $column->getType();
        }

        if ($column->getType() === 'boolean') {
            $def = $column->getType();
        }

        $aliases_for_TIMESTAMP_LTZ = ['timestamp_ltz', 'timestampltz', 'timestamp with local time zone'];
        $aliases_for_TIMESTAMP_NTZ = ['timestamp_ntz', 'datetime', 'timestampntz', 'timestamp without time zone'];
        $aliases_for_TIMESTAMP_TZ = ['timestamp_tz', 'timestamptz', 'timestamp with time zone'];
        $timestamps = array_merge(
            ['timestamp'], $aliases_for_TIMESTAMP_LTZ, $aliases_for_TIMESTAMP_NTZ, $aliases_for_TIMESTAMP_TZ
        );
        if (in_array($column->getType(), $timestamps)) {
            $def = 'timestamp';
            if (in_array($column->getType(), $aliases_for_TIMESTAMP_LTZ)) {
                $def = 'timestamp_ltz';
            }
            if (in_array($column->getType(), $aliases_for_TIMESTAMP_NTZ)) {
                $def = 'timestamp_ntz';
            }
            if (in_array($column->getType(), $aliases_for_TIMESTAMP_TZ)) {
                $def = 'timestamp_tz';
            }
            if ($column->getPrecision()) {
                $def .= "({$column->getPrecision()})";
            }
        }

        $column->isNull() ? $def .= ' null' : $def .= ' not null';

        if (!is_null($column->getDefault())) {
            if ('null' != $column->getDefault()) {
                $numeric = array_merge($synonymousWithNumber, $synonymousWithNumberWithoutPrecisionAndScale, $synonymousWithFloat);
                $string = array_merge($synonymousWithVarchar, ['time', 'date'], $timestamps);
                $functions = [
                    'current_timestamp', 'sysdate', 'convert_timezone', 'to_varchar',
                    'to_timestamp', 'to_timestamp_tz', 'to_timestamp_ntz'
                ];
                $matches = preg_grep("/" . implode("|", $functions) . "/i", array($column->getDefault()));
                if (!empty($matches)) {
                    $def .= ' default ' . $column->getDefault();
                } elseif (in_array($column->getType(), $numeric)) {
                    $def .= ' default ' . floatval($column->getDefault());
                } elseif (in_array($column->getType(), $string)) {
                    $def .= " default '{$column->getDefault()}'";
                } elseif ($column->getType() === 'boolean' && is_bool($column->getDefault())) {
                    $def .= ' default ' . ($column->getDefault() ? 'true' : 'false');
                }
            } else {
                $def .= ' default null';
            }
        }

        if ($column->getProperties()) {
            $def .= ' ' . implode(' ', $column->getProperties());
        }

        if ($column->getComment()) {
            $def .= " comment '{$column->getComment()}'";
        }

        return $def;
    }

    public function getVersionLog(): array
    {
        if (!isset($this->options['version_order'])) {
            throw new RuntimeException('Invalid version_order configuration option');
        }

        $orderBy = match ($this->options['version_order']) {
            Config::VERSION_ORDER_CREATION_TIME => "{$this->quoteColumnName('version')} asc",
            Config::VERSION_ORDER_EXECUTION_TIME =>
                "{$this->quoteColumnName('start_time')} asc, " .
                "{$this->quoteColumnName('version')} asc",
        };

        // This will throw an exception if doing a --dry-run without any migrations as phinxlog
        // does not exist, so in that case, we can just expect to trivially return empty set
        try {
            $rows = $this->fetchAll(sprintf(
                'select * from %s order by %s',
                $this->quoteTableName($this->getSchemaTableName()), $orderBy
            ));
        } catch (PDOException $e) {
            if (!$this->isDryRunEnabled()) {
                throw $e;
            }
            $rows = [];
        }

        $result = [];
        foreach ($rows as $version) {
            $version['breakpoint'] = in_array(
                $version['breakpoint'], [0, '0', false, 'false', '', null], true
            ) ? 0 : 1;
            $result[(int)$version['version']] = $version;
        }

        return $result;
    }

    public function getColumnTypes(): array
    {
        //Summary of Data Types
        //https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html

        //Numeric Data Types
        //https://docs.snowflake.com/en/sql-reference/data-types-numeric.html
        $numeric = [
            'number',
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
        ];

        //String & Binary Data Types
        //https://docs.snowflake.com/en/sql-reference/data-types-text.html
        $string = [
            'varchar',
            'char',
            'character',
            'string',
            'text',
            'binary',
            'varbinary',
        ];

        //Logical Data Types
        //https://docs.snowflake.com/en/sql-reference/data-types-logical.html
        $logical = [
            'boolean',
        ];

        //Date & Time Data Types
        //https://docs.snowflake.com/en/sql-reference/data-types-datetime.html
        $datetime = [
            'date',
            'datetime',
            'time',
            'timestamp',
            'timestamp_ltz',
            'timestamp_ntz',
            'timestamp_tz',
        ];

        //Semi-structured Data Types
        //https://docs.snowflake.com/en/sql-reference/data-types-semistructured.html
        $semistructured = [
            'variant',
            'object',
            'array',
        ];

        //Geospatial Data Types
        //https://docs.snowflake.com/en/sql-reference/data-types-geospatial.html
        $geospatial = [
            'geography',
            'geometry',
        ];

        return array_merge($numeric, $string, $logical, $datetime, $semistructured, $geospatial);

    }

    public function isValidColumnType(Column $column): bool
    {
        return in_array($column->getType(), $this->getColumnTypes(), true);
    }


}
