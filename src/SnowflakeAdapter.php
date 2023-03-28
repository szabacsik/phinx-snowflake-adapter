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
use InvalidArgumentException;

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
        $options = $table->getOptions();

        $hasIdColumn = array_filter($columns, function ($column) {
            return $column->getName() === 'id';
        });

        if (!$hasIdColumn && (!isset($options['id']) || ($options['id'] === true))) {
            $options['id'] = 'id';
        }

        if (isset($options['id']) && is_string($options['id'])) {
            $column = new Column();
            $column->setName($options['id'])
                ->setType('number')
                ->setOptions(['identity' => true])
                ->setProperties(['primary key']);
            if (isset($options['limit'])) {
                $column->setLimit($options['limit']);
            }
            array_unshift($columns, $column);
        }

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

        if (isset($table->getOptions()['unique'])) {
            $sql .= 'unique ("';
            $sql .= (
                is_array($table->getOptions()['unique'])
                    ? implode('", "', $table->getOptions()['unique'])
                    : $table->getOptions()['unique']
                ) . '")';
        }

        $sql = rtrim($sql, $separator);
        $sql .= ')';

        $this->execute($sql);
    }

    public function truncateTable(string $tableName): void
    {
        $this->execute(sprintf('truncate table %s', $this->quoteTableName($tableName)));
    }

    public function getColumns(string $tableName): array
    {
        $columns = [];
        $rows = $this->fetchAll(sprintf('show columns in table %s', $this->quoteTableName($tableName)));
        foreach ($rows as $row) {
            $name = $row['column_name'];
            $dataType = json_decode($row['data_type'], true);
            $types = ['varchar' => 'TEXT', 'number' => 'FIXED', 'float' => 'REAL'];
            $type = in_array($dataType['type'], $types) ? array_search($dataType['type'], $types) : strtolower($dataType['type']);
            $precision = $dataType['precision'] ?? null;
            $scale = $dataType['scale'] ?? null;
            $length = $dataType['length'] ?? null;
            $nullable = $dataType['nullable'] ?? $row['null?'] == 'true' ?? false;
            if (isset($row['default']) && !empty($row['default'])) {
                switch (gettype($row['default'])) {
                    case 'string' :
                        if (!in_array(strtolower($row['default']), ['true', 'false'])) {
                            $default = preg_replace("/^'|'$/", "", $row['default']);
                        } else {
                            $default = filter_var($row['default'], FILTER_VALIDATE_BOOLEAN);
                        }
                        break;
                    case 'NULL':
                        $default = null;
                        break;
                    default:
                        $default = $row['default'];
                        break;
                }
            } else {
                $default = null;
            }
            $comment = $row['comment'] ?? null;
            $autoincrement = $row['autoincrement'] ?? '';
            $identity = (bool)$autoincrement;
            preg_match_all('!\d+!', $autoincrement, $matches);
            $seed = $matches[0][0] ?? null;
            $increment = $matches[0][1] ?? null;

            $column = new Column();
            $column->setName($name);
            $column->setType($type);
            if (isset($dataType['precision'])) {
                $column->setPrecision($precision);
            }
            if (isset($dataType['length'])) {
                $column->setLimit($length);
            }
            $column->setScale($scale);
            $column->setNull($nullable);
            $column->setComment($comment);
            $column->setDefault($default);
            if (is_numeric($seed)) {
                $column->setSeed((int)$seed);
            }
            if (is_numeric($increment)) {
                $column->setSeed((int)$increment);
            }
            $column->setIdentity($identity);
            $columns[] = $column;
        }
        return $columns;
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
        return !empty($this->getPrimaryKey($tableName));
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

    /**
     * @link https://docs.snowflake.com/en/sql-reference/sql/alter-table.html
     */
    protected function getAddColumnInstructions(Table $table, Column $column): AlterInstructions
    {
        return new AlterInstructions([sprintf(
            'add %s %s',
            $this->quoteColumnName($column->getName()),
            $this->getColumnSqlDefinition($column)
        )]);
    }

    /**
     * @link https://docs.snowflake.com/en/sql-reference/sql/alter-table.html
     */
    protected function getRenameColumnInstructions(string $tableName, string $columnName, string $newColumnName): AlterInstructions
    {
        // rename column old_name to new_name;
        return new AlterInstructions([sprintf(
            'rename column %s to %s',
            $this->quoteColumnName($columnName),
            $this->quoteColumnName($newColumnName)
        )]);
    }

    protected function getChangeColumnInstructions(string $tableName, string $columnName, Column $newColumn): AlterInstructions
    {
        // https://docs.snowflake.com/en/sql-reference/sql/alter-table-column
        // https://book.cakephp.org/phinx/0/en/migrations.html#changing-column-attributes
        $currentColumn = null;
        $columns = $this->getColumns($tableName);
        foreach ($columns as $column) {
            if ($column->getName() === $columnName) {
                $currentColumn = $column;
                break;
            }
        }
        if (!$currentColumn) {
            throw new InvalidArgumentException("The specified column doesn't exist: $columnName");
        }

        $instruction = new AlterInstructions();

        if ($currentColumn->getType() != $newColumn->getType()) {
            $instruction->addAlter(sprintf(
                'alter column %s set data type %s',
                $this->quoteColumnName($currentColumn->getName()),
                $newColumn->getType()
            ));
        }

        if ($currentColumn->getDefault() != $newColumn->getDefault()) {
            switch (true) {
                case empty($newColumn->getDefault()):
                    $instruction->addAlter(sprintf(
                        'alter column %s drop default',
                        $this->quoteColumnName($currentColumn->getName())
                    ));
                    break;
                default:
                    $instruction->addAlter(sprintf(
                        'alter column %s set default %s',
                        $this->quoteColumnName($currentColumn->getName()),
                        $newColumn->getDefault()
                    ));
                    break;
            }
        }

        if ($currentColumn->getNull() != $newColumn->getNull()) {
            $newColumn->isNull()
                ? $sql = 'alter column %s drop not null'
                : $sql = 'alter column %s set not null';
            $instruction->addAlter(
                sprintf($sql, $this->quoteColumnName($columnName))
            );
        }

        if ($currentColumn->getComment() != $newColumn->getComment()) {
            switch (true) {
                case empty($newColumn->getComment()):
                    $sql = sprintf('alter %s unset comment', $this->quoteColumnName($columnName));
                    break;
                default:
                    $sql = sprintf(
                        "alter %s comment '%s'",
                        $this->quoteColumnName($columnName),
                        $newColumn->getComment()
                    );
            }
            $instruction->addAlter($sql);
        }

        if ('varchar' === $currentColumn->getType() && $currentColumn->getType() === $newColumn->getType()) {
            if ($currentColumn->getLimit() != $newColumn->getLimit()) {
                $sql = sprintf(
                    'alter column %s set data type %s(%s) %s',
                    $this->quoteColumnName($columnName),
                    $currentColumn->getType(),
                    $newColumn->getLimit(),
                    $currentColumn->getCollation() ? "collate '{$currentColumn->getCollation()}'" : ''
                );
                $instruction->addAlter(trim($sql));
            }
        }

        if ('number' === $currentColumn->getType() && $currentColumn->getType() === $newColumn->getType()) {
            if ($currentColumn->getPrecision() != $newColumn->getPrecision()) {
                $instruction->addAlter(sprintf(
                    'alter %s set data type number(%s,%s)',
                    $this->quoteColumnName($currentColumn->getName()),
                    $newColumn->getPrecision(),
                    $newColumn->getScale() ?? $currentColumn->getScale()
                ));
            }
        }

        return $instruction;

    }

    /**
     * @link https://docs.snowflake.com/en/sql-reference/sql/alter-table.html
     */
    protected function getDropColumnInstructions(string $tableName, string $columnName): AlterInstructions
    {
        return new AlterInstructions([sprintf('drop column %s', $this->quoteColumnName($columnName))]);
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
        $constraint = $foreignKey->getConstraint() ? ' constraint ' . $this->quoteColumnName($foreignKey->getConstraint()) : '';
        $columns = implode(',', array_map([$this, 'quoteColumnName'], $foreignKey->getColumns()));
        $referencedColumns = implode(',', array_map([$this, 'quoteColumnName'], $foreignKey->getReferencedColumns()));
        $referencedTable = $this->quoteTableName($foreignKey->getReferencedTable()->getName());
        $sql = sprintf('add%s foreign key (%s) references %s(%s)', $constraint, $columns, $referencedTable, $referencedColumns);
        return new AlterInstructions([$sql]);
    }

    protected function getDropForeignKeyInstructions(string $tableName, string $constraint): AlterInstructions
    {
        // TODO: Implement getDropForeignKeyInstructions() method.
    }

    protected function getDropForeignKeyByColumnsInstructions(string $tableName, array $columns): AlterInstructions
    {
        $sql = sprintf(
            'drop foreign key (%s)',
            implode(',', array_map([$this, 'quoteColumnName'], $columns))
        );
        return new AlterInstructions([$sql]);
    }

    protected function getDropTableInstructions(string $tableName): AlterInstructions
    {
        $sql = sprintf('drop table %s', $this->quoteTableName($tableName));
        return new AlterInstructions([], [$sql]);
    }

    /**
     * @link https://docs.snowflake.com/en/sql-reference/sql/alter-table.html#examples
     */
    protected function getRenameTableInstructions(string $tableName, string $newTableName): AlterInstructions
    {
        return new AlterInstructions([sprintf('rename to %s', $this->quoteTableName($newTableName))]);
    }

    protected function getChangePrimaryKeyInstructions(Table $table, $newColumns): AlterInstructions
    {
        $types = ['string', 'array', 'NULL'];
        if (!in_array(gettype($newColumns), $types)) {
            throw new \InvalidArgumentException(
                sprintf('The type of the $newColumns argument must be one of `%s`',
                    strtolower(implode('`,`', $types))
                )
            );
        }
        if (is_array($newColumns)) {
            if (array_filter($newColumns, 'is_string') != $newColumns) {
                throw new \InvalidArgumentException('Only strings are allowed in $newColumns array elements.');
            }
        }

        $instructions = new AlterInstructions();

        if ($this->hasPrimaryKey($table->getName(), [])) {
            $instructions->addAlter('drop primary key');
        }

        if (!$newColumns) {
            return $instructions;
        }

        if (is_string($newColumns)) {
            $newColumns = [$newColumns];
        }

        $sql = 'add primary key (';
        $sql .= implode(',', array_map([$this, 'quoteColumnName'], $newColumns));
        $sql .= ')';

        $instructions->addAlter($sql);

        return $instructions;
    }

    /**
     * @param Table $table
     * @param string|null $newComment
     * @return AlterInstructions
     * @see https://docs.snowflake.com/en/sql-reference/sql/comment.html
     */
    protected function getChangeCommentInstructions(Table $table, ?string $newComment): AlterInstructions
    {
        return new AlterInstructions([], [
            sprintf("comment on table %s is '%s'", $this->quoteTableName($table->getName()), $newComment)
        ]);
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
            if ('timestamp' === $column->getType()) {
                $column->getTimezone() ? $def = 'timestamp_tz' : $def = 'timestamp';
            } elseif (in_array($column->getType(), $aliases_for_TIMESTAMP_LTZ)) {
                $def = 'timestamp_ltz';
            } elseif (in_array($column->getType(), $aliases_for_TIMESTAMP_NTZ)) {
                $def = 'timestamp_ntz';
            } elseif (in_array($column->getType(), $aliases_for_TIMESTAMP_TZ)) {
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
                    'to_timestamp', 'to_timestamp_tz', 'to_timestamp_ntz',
                    'uuid_string',
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
            'biginteger',
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

    protected function executeAlterSteps(string $tableName, AlterInstructions $instructions): void
    {
        foreach ($instructions->getAlterParts() as $alterPart) {
            $this->execute(sprintf('alter table %s %s', $this->quoteTableName($tableName), $alterPart));
        }

        foreach ($instructions->getPostSteps() as $postStep) {
            if (is_string($postStep)) {
                $this->execute($postStep);
                continue;
            }
            if (is_callable($postStep)) {
                //TODO: Implement execute callable post steps
            }
        }
    }

    public function getPrimaryKey(string $tableName): array
    {
        return $this->fetchAll(sprintf('show primary keys in %s', $this->quoteTableName($tableName)));
    }

    public function bulkinsert(Table $table, array $rows): void
    {
        $current = current($rows);
        $keys = array_keys($current);
        $countRows = count($rows);
        $columnNames = implode(',', array_map([$this, 'quoteColumnName'], $keys));
        $namedParameters = [];
        $values = [];
        for ($i = 0; $i < $countRows; $i++) {
            $namedParameter = [];
            foreach ($keys as $key) {
                $namedParameter[] = ":$i$key";
                if ($this->isDryRunEnabled()) {
                    $values[":$i$key"] = $rows[$i][$key];
                }
            }
            $namedParameters[] = '(' . implode(',', $namedParameter) . ')';
        }
        $namedParameters = implode(',', $namedParameters);
        $sql = sprintf(
            'insert into %s (%s) values %%s',
            $this->quoteTableName($table->getName()),
            $columnNames
        );
        if ($this->isDryRunEnabled()) {
            foreach ($values as $key => $value) {
                $namedParameters = str_replace($key, "'$value'", $namedParameters);
            }
            $sql = sprintf($sql, $namedParameters);
            $this->output->writeln($sql);
        } else {
            $sql = sprintf($sql, $namedParameters);
            $stmt = $this->getConnection()->prepare($sql);
            $vals = [];
            foreach ($rows as $row) {
                foreach ($row as $v) {
                    if (is_bool($v)) {
                        $vals[] = $this->castToBool($v);
                    } else {
                        $vals[] = $v;
                    }
                }
            }
            $stmt->execute($vals);
        }
    }

}
