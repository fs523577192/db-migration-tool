/*
 * Copyright 2023
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.firas.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import tech.firas.db.vo.Column;
import tech.firas.db.vo.Index;
import tech.firas.db.vo.Schema;
import tech.firas.db.vo.Table;

@Slf4j
public class MigrationTool {

    public enum MigrateDataOption {
        NONE,
        TRUNCATE_FIRST,
        DELETE_ALL_FIRST
    }

    private DbMetaReader sourceReader;
    private DbMetaReader targetReader;
    private DbMetaWriter targetWriter;

    private Connection sourceConnection;
    private Connection targetConnection;

    @Getter private int dataBatchSize = 100;

    public MigrationTool(final DbMetaReader sourceReader, final Connection sourceConnection,
            final DbMetaReader targetReader, final DbMetaWriter targetWriter, final Connection targetConnection) {
        this.sourceReader = sourceReader;
        this.sourceConnection = sourceConnection;
        this.targetReader = targetReader;
        this.targetWriter = targetWriter;
        this.targetConnection = targetConnection;
    }

    public void setDataBatchSize(final int dataBatchSize) {
        if (dataBatchSize < 1) {
            throw new IllegalStateException("dataBatchSize must be at least 1");
        }
        this.dataBatchSize = dataBatchSize;
    }

    public boolean migrateTableStructure(final Table table) throws SQLException {
        final Table sourceTable = new Table(new Schema(table.getSchema().getName()), table.getName());
        sourceTable.setColumnMap(this.sourceReader.readColumns(this.sourceConnection, sourceTable));
        sourceTable.setIndexMap(this.sourceReader.readIndexes(this.sourceConnection, sourceTable));

        final Table targetTable = new Table(new Schema(table.getSchema().getName()), table.getName());
        final Map<String, Column> columnMap = this.targetReader.readColumns(this.targetConnection, targetTable);
        if (columnMap == null || columnMap.isEmpty()) {
            log.info("The table {} does not exist in the target DB", this.targetReader.tableName(targetTable));
            this.targetWriter.createTable(this.targetConnection, sourceTable);
            return true;
        } else {
            log.info("The table {} already exists in the target DB", this.targetReader.tableName(targetTable));
            targetTable.setColumnMap(columnMap);
            for (final Column sourceColumn : sourceTable.getColumnMap().values()) {
                if (columnMap.keySet().stream().noneMatch(
                        columnName -> sourceColumn.getName().equalsIgnoreCase(columnName)
                )) {
                    this.targetWriter.createColumn(targetConnection, sourceColumn);
                }
            }
            final Map<String, Index> indexMap = this.targetReader.readIndexes(this.targetConnection, targetTable);
            for (final Index sourceIndex : sourceTable.getIndexMap().values()) {
                if (indexMap.values().stream().noneMatch(
                        targetIndex -> sourceIndex.getName().equalsIgnoreCase(targetIndex.getName())
                )) {
                    this.targetWriter.createIndex(targetConnection, sourceIndex);
                }
            }
            return false;
        }
    }

    public void migrateTableData(final Table table) throws SQLException {
        final String insertSql = this.targetWriter.insertSqlFor(table);
        final String selectAllSql = this.sourceReader.selectAllSqlFor(table);
        if (log.isDebugEnabled()) {
            log.debug("Before execute from source: {}", selectAllSql);
        } else if (log.isInfoEnabled()) {
            log.info("Before selectAll from {}", this.sourceReader.tableName(table));
        }
        try (final Statement srcSt = this.sourceConnection.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            srcSt.setFetchSize(this.dataBatchSize);
            try (final ResultSet srcRs = srcSt.executeQuery(selectAllSql)) {
                final String targetTableName = this.targetWriter.tableName(table);
                if (log.isDebugEnabled()) {
                    log.debug("Before execute from target: {}", selectAllSql);
                } else if (log.isInfoEnabled()) {
                    log.info("Before insert into {}", targetTableName);
                }
                migrateTableDataFromSrcResultSet(table, insertSql, srcRs, targetTableName);
                if (log.isDebugEnabled()) {
                    log.debug("After execute on target: {}", selectAllSql);
                } else if (log.isInfoEnabled()) {
                    log.info("After insert into {}", targetTableName);
                }
            }
        }
    }

    private void migrateTableDataFromSrcResultSet(final Table table, final String insertSql,
            final ResultSet srcRs, final String targetTableName) throws SQLException {
        try (final PreparedStatement destPs = this.targetConnection.prepareStatement(insertSql)) {
            int count = 0;
            while (srcRs.next()) {
                insertOneRow(table, srcRs, destPs);
                ++count;
                if (log.isTraceEnabled()) {
                    log.trace("{} row(s) inserted into {}", count, targetTableName);
                }
                if (count % this.dataBatchSize == 0) {
                    destPs.executeBatch();
                    if (log.isDebugEnabled()) {
                        log.debug("A batch executed to insert into {}", targetTableName);
                    }
                }
            }
            if (count % this.dataBatchSize != 0) {
                destPs.executeBatch();
                if (log.isDebugEnabled()) {
                    log.debug("A batch executed to insert into {}", targetTableName);
                }
            }
        }
    }

    private static void insertOneRow(final Table table, final ResultSet srcRs, final PreparedStatement destPs)
            throws SQLException {
        int i = 1;
        for (final Column column : table.getColumnMap().values()) {
            column.getDataType().setPreparedStatementParameter(destPs, i++, column.getFromResultSet(srcRs));
        }
        destPs.addBatch();
    }

    public void migrateTableStructureWithData(final Table table, final MigrateDataOption migrateDataOption)
            throws SQLException {
        if (this.migrateTableStructure(table)) {
            if (MigrateDataOption.TRUNCATE_FIRST == migrateDataOption) {
                try (final Statement statement = this.targetConnection.createStatement()) {
                    log.debug("Truncate table {} before migrating data", table);
                    statement.executeUpdate(this.targetWriter.truncateTableSqlFor(table));
                }
            } else if (MigrateDataOption.DELETE_ALL_FIRST == migrateDataOption) {
                try (final Statement statement = this.targetConnection.createStatement()) {
                    log.debug("Delete all from table {} before migrating data", table);
                    statement.executeUpdate(this.targetWriter.deleteAllSqlFor(table));
                }
            }
            this.migrateTableData(table);
        }
    }
}
