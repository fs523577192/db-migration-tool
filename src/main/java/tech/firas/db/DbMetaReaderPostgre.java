package tech.firas.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import tech.firas.db.datatype.BigIntType;
import tech.firas.db.datatype.BlobType;
import tech.firas.db.datatype.CharType;
import tech.firas.db.datatype.ClobType;
import tech.firas.db.datatype.DataType;
import tech.firas.db.datatype.DateType;
import tech.firas.db.datatype.DecimalType;
import tech.firas.db.datatype.IntegerType;
import tech.firas.db.datatype.SmallIntType;
import tech.firas.db.datatype.TimeType;
import tech.firas.db.datatype.TimestampType;
import tech.firas.db.datatype.UnknownType;
import tech.firas.db.datatype.VarCharType;
import tech.firas.db.vo.Column;
import tech.firas.db.vo.Index;
import tech.firas.db.vo.Index.IndexType;
import tech.firas.db.vo.Schema;
import tech.firas.db.vo.Table;

@Slf4j
public class DbMetaReaderPostgre extends AbstractDbMetaReader {

    private static final Pattern INDEX_PATTERN = Pattern.compile("^CREATE (UNIQUE )?INDEX (\"?)(\\w+)\\2 " +
            "ON (\\w+)\\.(\\w+) " +
            "USING (\\w+) \\((\\w+(?:, \\w+)*)\\)");

    @Override
    public Set<Schema> read(final Connection connection) throws SQLException {
        final Set<Schema> result = new LinkedHashSet<>();
        try (final Statement statement = connection.createStatement()) {
            try (final ResultSet resultSet = statement.executeQuery(
                    "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")) {
                while (resultSet.next()) {
                    final Schema schema = new Schema();
                    schema.setName(resultSet.getString("schema_name"));
                    result.add(schema);
                }
            }
        }
        for (final Schema schema : result) {
            schema.setTables(this.readTables(connection, schema));
        }
        return result;
    }

    @Override
    public Set<Table> readTables(final Connection connection, final Schema schema) throws SQLException {
        final Set<Table> result = new LinkedHashSet<>();
        try (final PreparedStatement ps = connection.prepareStatement(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name")) {
            ps.setString(1, schema.getName().toLowerCase(Locale.US));
            try (final ResultSet resultSet = ps.executeQuery()) {
                while (resultSet.next()) {
                    final Table table = new Table();
                    table.setSchema(schema);
                    table.setName(resultSet.getString("table_name"));
                    result.add(table);
                }
            }
        }
        for (final Table table : result) {
            table.setColumnMap(this.readColumns(connection, table));
        }
        for (final Table table : result) {
            table.setIndexMap(this.readIndexes(connection, table));
        }
        return result;
    }

    @Override
    public Map<String, Column> readColumns(final Connection connection, final Table table) throws SQLException {
        try (final PreparedStatement ps = connection.prepareStatement(
                "SELECT column_name, data_type, character_maximum_length, " +
                        "numeric_precision, numeric_scale, datetime_precision, column_default, is_nullable " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
            ps.setString(1, table.getSchema().getName().toLowerCase(Locale.US));
            ps.setString(2, table.getName().toLowerCase(Locale.US));
            try (final ResultSet resultSet = ps.executeQuery()) {
                final Map<String, Column> result = new LinkedHashMap<>();
                while (resultSet.next()) {
                    final Column column = new Column();
                    column.setTable(table);
                    column.setName(resultSet.getString("column_name"));
                    column.setNotNull("NO".equals(resultSet.getString("is_nullable")));
                    column.setDataType(readDataType(resultSet));
                    result.put(column.getName(), column);
                }
                return result;
            }
        }
    }

    @Override
    public Map<String, Index> readIndexes(final Connection connection, final Table table) throws SQLException {
        try (final PreparedStatement ps = connection.prepareStatement(
                "SELECT p.indexName, p.indexDef, c.constraint_type " +
                        "FROM pg_indexes p " +
                        "LEFT JOIN information_schema.table_constraints c " +
                        "ON c.table_schema = p.schemaName AND c.table_name = p.tableName AND c.constraint_name = p.indexName " +
                        "WHERE p.schemaName = ? AND p.tableName = ? " +
                        "GROUP BY p.indexName, p.indexDef, c.constraint_type ORDER BY p.indexName")) {
            ps.setString(1, table.getSchema().getName().toLowerCase(Locale.US));
            ps.setString(2, table.getName().toLowerCase(Locale.US));
            try (final ResultSet resultSet = ps.executeQuery()) {
                final Map<String, Index> result = new LinkedHashMap<>();
                while (resultSet.next()) {
                    final Index index = new Index();
                    index.setTable(table);
                    index.setName(resultSet.getString("indexName"));
                    readIndex(index, resultSet.getString("indexDef"),
                            resultSet.getString("constraint_type"));
                    result.put(index.getName(), index);
                }
                return result;
            }
        }
    }

    private static DataType readDataType(final ResultSet resultSet) throws SQLException {
        final String typeName = resultSet.getString("data_type");
        if ("integer".equals(typeName)) {
            return IntegerType.instance;
        } else if ("bigint".equals(typeName)) {
            return BigIntType.instance;
        } else if ("smallint".equals(typeName)) {
            return SmallIntType.instance;

        } else if ("numeric".equals(typeName)) {
            final DecimalType decimalType = new DecimalType();
            decimalType.setPrecision(resultSet.getInt("numeric_precision"));
            decimalType.setScale(resultSet.getInt("numeric_scale"));
            return decimalType;

        } else if ("character varying".equals(typeName)) {
            final VarCharType varCharType = new VarCharType();
            varCharType.setLength(resultSet.getInt("character_maximum_length"));
            return varCharType;
        } else if ("character".equals(typeName)) {
            final CharType charType = new CharType();
            charType.setLength(resultSet.getInt("character_maximum_length"));
            return charType;

        } else if ("timestamp without time zone".equals(typeName)) {
            final TimestampType timestampType = new TimestampType();
            timestampType.setPrecision(resultSet.getInt("datetime_precision"));
            return timestampType;
        } else if ("date".equals(typeName)) {
            return DateType.instance;
        } else if ("time without time zone".equals(typeName)) {
            final TimeType timeType = new TimeType();
            timeType.setPrecision(resultSet.getInt("datetime_precision"));
            return timeType;

        } else if ("text".equals(typeName)) {
            return ClobType.instance;

        } else if ("bytea".equals(typeName)) {
            return BlobType.instance;

        } else {
            log.debug("Unknown type: {}", typeName);
            final UnknownType unknownType = new UnknownType();
            unknownType.setName(typeName);
            return unknownType;
        }
    }

    private static void readIndex(final Index index, final String indexDefinition, final String constraintType) {
        final Matcher matcher = INDEX_PATTERN.matcher(indexDefinition);
        if (!matcher.find()) {
            throw new IllegalStateException("Invalid index definition: " + indexDefinition);
        }

        if (log.isDebugEnabled() && "\"".equals(matcher.group(2))) {
            log.debug("Quoted index name: {}", matcher.group(3));
        }

        if ("PRIMARY_KEY".equals(constraintType)) {
            index.setIndexType(IndexType.PRIMARY_KEY);
        } else if ("UNIQUE".equals(matcher.group(1))) {
            log.debug("unique index {}.{}, constraint type: {}", index.getTable().getSchema().getName(),
                    index.getName(), constraintType);
            index.setIndexType(IndexType.UNIQUE_KEY);
        } else {
            log.debug("normal index {}.{}, constraint type: {}", index.getTable().getSchema().getName(),
                    index.getName(), constraintType);
            index.setIndexType(IndexType.NORMAL);
        }

        final Map<String, Column> columnMap = index.getTable().getColumnMap();
        final String[] columnArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(matcher.group(7), ", ");
        final List<Column> columnList = new ArrayList<>(columnArray.length);
        for (final String columnName : columnArray) {
            final Column column = columnMap.get(columnName);
            columnList.add(column);
        }
        index.setColumns(columnList);
    }
}
