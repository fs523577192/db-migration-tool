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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import tech.firas.db.datatype.BigIntType;
import tech.firas.db.datatype.BlobType;
import tech.firas.db.datatype.CharType;
import tech.firas.db.datatype.ClobType;
import tech.firas.db.datatype.DataType;
import tech.firas.db.datatype.DateType;
import tech.firas.db.datatype.DecimalType;
import tech.firas.db.datatype.DoubleType;
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

/**
 * For identifier case sensitivity, refer to
 * https://dev.mysql.com/doc/refman/8.0/en/identifier-case-sensitivity.html and
 * https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html
 */
@Slf4j
public class DbMetaReaderMySql extends AbstractDbMetaReader {

    /**
     * Refer to https://dev.mysql.com/doc/refman/8.0/en/information-schema-schemata-table.html
     * @param connection the DB Connection
     * @return a Set of Schema in the MySQL database
     * @throws SQLException if it failed to query MySQL
     */
    @Override
    public Set<Schema> read(final Connection connection) throws SQLException {
        final Set<Schema> result = new LinkedHashSet<>();
        try (final Statement statement = connection.createStatement()) {
            try (final ResultSet resultSet = statement.executeQuery(
                    "SELECT schema_name FROM information_schema.schemata " +
                            // excluding system schema
                            "WHERE schema_name NOT IN ('information_schema', 'performance_schema') " +
                            "ORDER BY schema_name")) {
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

    /**
     * Refer to https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html
     * @param connection the DB Connection
     * @param schema the Schema
     * @return a Set of Table in the specified Schema
     * @throws SQLException if it failed to query MySQL
     */
    @Override
    public Set<Table> readTables(final Connection connection, final Schema schema) throws SQLException {
        final Set<Table> result = new LinkedHashSet<>();
        try (final PreparedStatement ps = connection.prepareStatement(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name")) {
            ps.setString(1, schema.getName());
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

    /**
     * Refer to https://www.postgresql.org/docs/13/infoschema-columns.html
     * @param connection the DB Connection
     * @param table the Table
     * @return a LinkedHashMap with column name as key and Column as value
     * @throws SQLException if it failed to query MySQL
     */
    @Override
    public LinkedHashMap<String, Column> readColumns(final Connection connection, final Table table) throws SQLException {
        try (final PreparedStatement ps = connection.prepareStatement(
                "SELECT column_name, data_type, character_maximum_length, " +
                        "numeric_precision, numeric_scale, datetime_precision, column_default, is_nullable " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
            ps.setString(1, table.getSchema().getName());
            ps.setString(2, table.getName());
            try (final ResultSet resultSet = ps.executeQuery()) {
                final LinkedHashMap<String, Column> result = new LinkedHashMap<>();
                while (resultSet.next()) {
                    final Column column = new Column();
                    column.setTable(table);
                    column.setName(resultSet.getString("column_name"));
                    column.setNotNull( "NO".equals(resultSet.getString("is_nullable")) );
                    column.setDataType(readDataType(resultSet));
                    result.put(column.getName(), column);
                }
                return result;
            }
        }
    }

    /**
     * Refer to https://www.postgresql.org/docs/13/view-pg-indexes.html
     * and https://www.postgresql.org/docs/13/infoschema-table-constraints.html
     * @param connection the DB Connection
     * @param table the Table
     * @return a Map with index name as key and Index as value
     * @throws SQLException if it failed to query MySQL
     */
    @Override
    public Map<String, Index> readIndexes(final Connection connection, final Table table) throws SQLException {
        try (final PreparedStatement ps = connection.prepareStatement(
                "SELECT s.index_name, s.non_unique, s.column_name, c.constraint_type " +
                        "FROM information_schema.statistics s " +
                        "LEFT JOIN information_schema.table_constraints c " +
                        "ON c.table_schema = s.table_schema AND c.table_name = s.table_name AND c.constraint_name = s.index_name " +
                        "WHERE s.table_schema = ? AND s.table_name = ? " +
                        "ORDER BY s.index_name, s.seq_in_index, c.constraint_type")) {
            ps.setString(1, table.getSchema().getName());
            ps.setString(2, table.getName());
            try (final ResultSet resultSet = ps.executeQuery()) {
                final Map<String, Index> result = new LinkedHashMap<>();
                while (resultSet.next()) {
                    final String indexName = resultSet.getString("index_name");
                    final boolean unique = resultSet.getInt("non_unique") == 0;
                    final String constraintType = resultSet.getString("constraint_type");
                    final Index index = result.computeIfAbsent(indexName, k -> {
                        final Index i = new Index();
                        i.setTable(table);
                        i.setName(indexName);
                        i.setColumns(new LinkedList<>());
                        readIndex(i, unique, constraintType);
                        return i;
                    });
                    index.getColumns().add(
                            table.getColumnMap().get(resultSet.getString("column_name"))
                    );
                }
                return result;
            }
        }
    }

    /**
     *
     * @param identifier the identifier to be quoted
     * @return the quoted identifier for MySQL (quoted with back quote '`')
     */
    @Override
    public String quote(final String identifier) {
        return '`' + identifier + '`'; // TODO: complicated case with back quote in the identifier itself
    }

    /**
     * See https://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html
     */
    private static DataType readDataType(final ResultSet resultSet) throws SQLException {
        final String typeName = resultSet.getString("data_type");
        if ("int".equals(typeName)) {
            return IntegerType.instance;
        } else if ("bigint".equals(typeName)) {
            return BigIntType.instance;
        } else if ("smallint".equals(typeName) || "tinyint".equals(typeName)) {
            return SmallIntType.instance;

        } else if ("double".equals(typeName)) {
            return DoubleType.instance;

        } else if ("decimal".equals(typeName)) {
            final DecimalType decimalType = new DecimalType();
            decimalType.setPrecision(resultSet.getInt("numeric_precision"));
            decimalType.setScale(resultSet.getInt("numeric_scale"));
            return decimalType;

        } else if ("varchar".equals(typeName)) {
            final VarCharType varCharType = new VarCharType();
            varCharType.setLength(resultSet.getInt("character_maximum_length"));
            return varCharType;
        } else if ("character".equals(typeName)) {
            final CharType charType = new CharType();
            charType.setLength(resultSet.getInt("character_maximum_length"));
            return charType;

        } else if ("datetime".equals(typeName) || "timestamp".equals(typeName)) {
            final TimestampType timestampType = new TimestampType();
            timestampType.setPrecision(resultSet.getInt("datetime_precision"));
            return timestampType;
        } else if ("date".equals(typeName)) {
            return DateType.instance;
        } else if ("time".equals(typeName)) {
            final TimeType timeType = new TimeType();
            timeType.setPrecision(resultSet.getInt("datetime_precision"));
            return timeType;

        } else if ("text".equals(typeName) || "longtext".equals(typeName)) {
            return ClobType.instance;

        } else if ("blob".equals(typeName)) {
            return BlobType.instance;

        } else {
            log.debug("Unknown type: {}", typeName);
            final UnknownType unknownType = new UnknownType();
            unknownType.setName(typeName);
            return unknownType;
        }
    }

    private static void readIndex(final Index index, final boolean unique, final String constraintType) {
        if ("PRIMARY KEY".equals(constraintType)) {
            index.setIndexType(IndexType.PRIMARY_KEY);
        } else if (unique) {
            log.debug("unique index {}.{}, constraint type: {}", index.getTable().getSchema().getName(),
                    index.getName(), constraintType);
            index.setIndexType(IndexType.UNIQUE_KEY);
        } else {
            log.debug("normal index {}.{}, constraint type: {}", index.getTable().getSchema().getName(),
                    index.getName(), constraintType);
            index.setIndexType(IndexType.NORMAL);
        }
    }
}
