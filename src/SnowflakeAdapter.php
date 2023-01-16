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
        // TODO: Implement beginTransaction() method.
    }

    public function commitTransaction(): void
    {
        // TODO: Implement commitTransaction() method.
    }

    public function rollbackTransaction(): void
    {
        // TODO: Implement rollbackTransaction() method.
    }

    public function quoteTableName(string $tableName): string
    {
        // TODO: Implement quoteTableName() method.
    }

    public function quoteColumnName(string $columnName): string
    {
        // TODO: Implement quoteColumnName() method.
    }

    public function hasTable(string $tableName): bool
    {
        // TODO: Implement hasTable() method.
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
