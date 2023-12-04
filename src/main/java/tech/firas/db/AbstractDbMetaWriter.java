package tech.firas.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.firas.db.datatype.BigIntType;
import tech.firas.db.datatype.CharType;
import tech.firas.db.datatype.DataType;
import tech.firas.db.datatype.DateType;
import tech.firas.db.datatype.IntegerType;
import tech.firas.db.datatype.SmallIntType;
import tech.firas.db.datatype.TimeType;
import tech.firas.db.datatype.TimestampType;
import tech.firas.db.datatype.VarCharType;
import tech.firas.db.vo.Column;
import tech.firas.db.vo.Index;
import tech.firas.db.vo.Index.IndexType;
import tech.firas.db.vo.Schema;
import tech.firas.db.vo.Table;

@Slf4j
public abstract class AbstractDbMetaWriter extends AbstractDbMetaUtil implements DbMetaWriter {

    @Override
    public String dataTypeToString(final DataType dataType) {
        if (dataType instanceof IntegerType) {
            return "INT";
        } else if (dataType instanceof BigIntType) {
            return "BIGINT";
        } else if (dataType instanceof SmallIntType) {
            return "SMALLINT";

        } else if (dataType instanceof DateType) {
            return "DATE";
        } else if (dataType instanceof TimeType) {
            final TimeType timeType = (TimeType) dataType;
            return "TIME" + (timeType.getPrecision() > 0 ? "(" + timeType.getPrecision() + ')' : "");
        } else if (dataType instanceof TimestampType) {
            final TimestampType timestampType = (TimestampType) dataType;
            return "TIMESTAMP" + (timestampType.getPrecision() > 0 ? "(" + timestampType.getPrecision() + ')' : "");

        } else if (dataType instanceof CharType) {
            final CharType charType = (CharType) dataType;
            return "CHAR" + (charType.getLength() > 0 ? "(" + charType.getLength() + ')' : "");
        } else if (dataType instanceof VarCharType) {
            final VarCharType varCharType = (VarCharType) dataType;
            return "VARCHAR" + (varCharType.getLength() > 0 ? "(" + varCharType.getLength() + ')' : "");

        } else {
            throw new UnsupportedOperationException(dataType + " is not supported");
        }
    }

    @Override
    public String columnInCreateTable(final Column column) {
        Objects.requireNonNull(column, "column must not be null");
        return column.getName() + ' ' + this.dataTypeToString(column.getDataType()) +
                (column.isNotNull() ? " NOT NULL" : "");
    }

    @Override
    public void createIndex(final Connection connection, final Index index) throws SQLException {
        if (index.getIndexType() == IndexType.PRIMARY_KEY) {
            log.info("Creating primary key is not supported");
            return;
        }
        try (final Statement statement = connection.createStatement()) {
            final String sql = this.createStatementFor(index);
            if (log.isDebugEnabled()) {
                log.debug("Before execute {}", sql);
            } else if (log.isInfoEnabled()) {
                log.info("Before create index {}.{}", this.tableName(index.getTable()), index.getName());
            }
            statement.executeUpdate(sql);
            if (log.isDebugEnabled()) {
                log.debug("After execute {}", sql);
            } else if (log.isInfoEnabled()) {
                log.info("After create index {}.{}", this.tableName(index.getTable()), index.getName());
            }
        }
    }

    @Override
    public String createStatementFor(final Index index) {
        Objects.requireNonNull(index, "index must not be null");
        Objects.requireNonNull(index.getTable(), "index.table must not be null");
        Objects.requireNonNull(index.getColumns(), "index.columns must not be null");
        if (index.getIndexType() == IndexType.PRIMARY_KEY) {
            return createPrimaryKeySql(index);
        }
        final String columns = index.getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.joining(", "));
        final StringBuilder builder = new StringBuilder("CREATE ");
        if (index.getIndexType() == IndexType.UNIQUE_KEY) {
            builder.append("UNIQUE ");
        }
        builder.append("INDEX ");
        if (index.getName() != null) {
            builder.append(index.getName()).append(' ');
        }
        builder.append("ON ").append(this.tableName(index.getTable())).append(" (").append(columns).append(')');
        return builder.toString();
    }

    protected String createPrimaryKeySql(final Index index) {
        final String columns = index.getColumns().stream()
                .map(Column::getName)
                .collect(Collectors.joining(", "));
        final StringBuilder builder = new StringBuilder("ALTER TABLE ")
                .append(this.tableName(index.getTable())).append(" ADD");
        if (index.getName() != null) {
            builder.append(" CONSTRAINT ").append(index.getName());
        }
        builder.append(" PRIMARY KEY (").append(columns).append(')');
        return builder.toString();
    }

    @Override
    public void createTable(final Connection connection, final Table table) throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            if (log.isInfoEnabled()) {
                log.info("Before create table {}", this.tableName(table));
            }
            for (final String sql : this.createStatementsFor(table)) {
                if (log.isDebugEnabled()) {
                    log.debug("Before execute {}", sql);
                }
                statement.executeUpdate(sql);
                if (log.isDebugEnabled()) {
                    log.debug("After execute {}", sql);
                }
            }
            if (log.isInfoEnabled()) {
                log.info("After create table {}", this.tableName(table));
            }
        }
    }

    @Override
    public List<String> createStatementsFor(final Table table) {
        return createStatementsFor(table, false);
    }

    protected List<String> createStatementsFor(final Table table, final boolean ifNotExists) {
        final String tableName = this.tableName(table);
        Objects.requireNonNull(table.getColumnMap(), "table.columnMap must not be null");

        final String columnDef = table.getColumnMap().values().stream()
                .map(this::columnInCreateTable).collect(Collectors.joining(",\n  "));

        final StringBuilder createTable = new StringBuilder("CREATE TABLE ");
        if (ifNotExists) {
            createTable.append("IF NOT EXISTS ");
        }
        createTable.append(tableName).append(" (\n  ").append(columnDef);

        final List<Index> normalIndexes = new LinkedList<>();
        if (table.getIndexMap() != null) {
            for (final Index index : table.getIndexMap().values()) {
                if (index.getIndexType() == IndexType.PRIMARY_KEY) {
                    constraintInCreateTable(createTable, index.getName(), "PRIMARY KEY (", index.getColumns());
                } else if (index.getIndexType() == IndexType.UNIQUE_KEY) {
                    constraintInCreateTable(createTable, index.getName(), "UNIQUE (", index.getColumns());
                } else {
                    normalIndexes.add(index);
                }
            }
        }
        final List<String> result = new ArrayList<>(1 + normalIndexes.size());
        result.add(createTable.append("\n)").toString());
        for (final Index index : normalIndexes) {
            result.add(this.createStatementFor(index));
        }
        return result;
    }

    @Override
    public void createColumn(final Connection connection, final Column column) throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            if (log.isInfoEnabled()) {
                log.info("Before create column {}.{}", this.tableName(column.getTable()), column.getName());
            }
            final String sql = this.createStatementFor(column);
            if (log.isDebugEnabled()) {
                log.debug("Before execute {}", sql);
            } else if (log.isInfoEnabled()) {
                log.info("Before create column {}.{}", this.tableName(column.getTable()), column.getName());
            }
            statement.executeUpdate(sql);
            if (log.isDebugEnabled()) {
                log.debug("After execute {}", sql);
            } else if (log.isInfoEnabled()) {
                log.info("After create column {}.{}", this.tableName(column.getTable()), column.getName());
            }
        }
    }

    @Override
    public String createStatementFor(final Column column) {
        return "ALTER TABLE " + this.tableName(column.getTable()) + " ADD COLUMN " +
                column.getName() + ' ' + this.dataTypeToString(column.getDataType()) +
                (column.isNotNull() ? " NOT NULL" : "");
    }

    @Override
    public void createSchema(final Connection connection, final Schema schema) throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            final String sql = this.createStatementFor(schema);
            if (log.isDebugEnabled()) {
                log.debug("Before execute {}", sql);
            } else if (log.isInfoEnabled()) {
                log.info("Before create schema {}", schema.getName());
            }
            statement.executeUpdate(sql);
            if (log.isDebugEnabled()) {
                log.debug("After execute {}", sql);
            } else if (log.isInfoEnabled()) {
                log.info("After create schema {}", schema.getName());
            }
        }
    }

    @Override
    public String createStatementFor(final Schema schema) {
        return createStatementFor(schema, false);
    }

    protected String createStatementFor(final Schema schema, final boolean ifNotExists) {
        final StringBuilder builder = new StringBuilder("CREATE SCHEMA ");
        if (ifNotExists) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(schema.getName());
        return builder.toString();
    }

    @Override
    public String deleteAllSqlFor(final Table table) {
        return "DELETE FROM " + this.tableName(table) + " WHERE 1 = 1";
    }

    @Override
    public String deleteByPrimaryKeySqlFor(final Table table) {
        return "DELETE FROM " + this.tableName(table) + this.whereSqlForPrimaryKey(table);
    }

    @Override
    public String insertSqlFor(final Table table) {
        final Set<String> columnNames = table.getColumnMap().keySet();
        return "INSERT INTO " + this.tableName(table) + " (" +
                String.join(", ", columnNames) + ")\nVALUES (" +
                columnNames.stream().map(c -> "?").collect(Collectors.joining(", ")) + ')';
    }

    @Override
    public String updateByPrimaryKeySqlFor(final Table table) {
        final Collection<Column> primaryKeyColumns = table.getPrimaryKeyColumns();
        return "UPDATE " + this.tableName(table) + " SET " +
                table.getColumnMap().values().stream()
                        .filter(column -> !primaryKeyColumns.contains(column))
                        .map(column -> column.getName() + " = ?")
                        .collect(Collectors.joining(", ")) +
                '\n' + this.whereSqlForPrimaryKey(table);
    }

    @Override
    public String truncateTableSqlFor(final Table table) {
        return "TRUNCATE TABLE " + this.tableName(table);
    }

    private static void constraintInCreateTable(final StringBuilder createTable, final String indexName,
            final String constraintType, final List<Column> columns) {
        createTable.append(",\n  ");
        if (indexName != null) {
            createTable.append("CONSTRAINT ").append(indexName).append(' ');
        }
        createTable.append(constraintType);
        createTable.append(
                columns.stream()
                        .map(Column::getName)
                        .collect(Collectors.joining(", "))
        );
        createTable.append(')');
    }
}
