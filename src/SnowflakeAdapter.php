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
        // TODO: Implement createTable() method.
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
        // TODO: Implement hasColumn() method.
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
}
