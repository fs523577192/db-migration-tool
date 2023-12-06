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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import tech.firas.db.datatype.BigIntType;
import tech.firas.db.datatype.BlobType;
import tech.firas.db.datatype.CharType;
import tech.firas.db.datatype.ClobType;
import tech.firas.db.datatype.DataType;
import tech.firas.db.datatype.DateType;
import tech.firas.db.datatype.DecimalType;
import tech.firas.db.datatype.IntegerType;
import tech.firas.db.datatype.TimeType;
import tech.firas.db.datatype.TimestampType;
import tech.firas.db.datatype.UnknownType;
import tech.firas.db.datatype.VarCharType;
import tech.firas.db.vo.Column;
import tech.firas.db.vo.Index;
import tech.firas.db.vo.Index.IndexType;
import tech.firas.db.vo.Schema;
import tech.firas.db.vo.Table;

public class DbMetaReaderDB2 extends AbstractDbMetaReader {

    /**
     * Refer to https://www.ibm.com/docs/en/db2/9.7?topic=views-syscatschemata
     * and https://www.ibm.com/docs/en/db2/11.1?topic=views-syscatschemata
     * @param connection the DB Connection
     * @return a Set of Schema in the DB2 database
     * @throws SQLException if it failed to query DB2
     */
    @Override
    public Set<Schema> read(final Connection connection) throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            try (final ResultSet resultSet = statement.executeQuery(
                    "select schemaName, REMARKS from SYSCAT.SCHEMATA " +
                            // excluding system schema
                            "WHERE DEFINER <> 'SYSIBM' order by schemaName")) {
                final Set<Schema> result = new LinkedHashSet<>();
                while (resultSet.next()) {
                    final Schema schema = new Schema();
                    schema.setName(resultSet.getString("schemaName"));
                    schema.setComment(resultSet.getString("REMARKS"));
                    schema.setTables(this.readTables(connection, schema));
                    result.add(schema);
                }
                return result;
            }
        }
    }

    /**
     * Refer to https://www.ibm.com/docs/en/db2/9.7?topic=views-syscattables
     * and https://www.ibm.com/docs/en/db2/11.1?topic=views-syscattables
     * @param connection the DB Connection
     * @param schema the Schema
     * @return a Set of Table in the specified Schema
     * @throws SQLException if it failed to query DB2
     */
    @Override
    public Set<Table> readTables(final Connection connection, final Schema schema) throws SQLException {
        try (final PreparedStatement ps = connection.prepareStatement(
                "select tabName, REMARKS from SYSCAT.TABLES " +
                        "where tabSchema = ? and \"TYPE\" = 'T' order by tabName")) {
            ps.setString(1, schema.getName());
            try (final ResultSet resultSet = ps.executeQuery()) {
                final Set<Table> result = new LinkedHashSet<>();
                while (resultSet.next()) {
                    final Table table = new Table();
                    table.setSchema(schema);
                    table.setName(resultSet.getString("tabName"));
                    table.setComment(resultSet.getString("REMARKS"));
                    table.setColumnMap(this.readColumns(connection, table));
                    // setColumnMap must be before setIndexMap, as setIndexMap may use table.columnMap
                    table.setIndexMap(this.readIndexes(connection, table));
                    result.add(table);
                }
                return result;
            }
        }
    }

    /**
     * Refer to https://www.ibm.com/docs/en/db2/9.7?topic=views-syscatcolumns
     * and https://www.ibm.com/docs/en/db2/11.1?topic=views-syscatcolumns
     * @param connection the DB Connection
     * @param table the Table
     * @return a LinkedHashMap with column name as key and Column as value
     * @throws SQLException if it failed to query DB2
     */
    @Override
    public LinkedHashMap<String, Column> readColumns(final Connection connection, final Table table) throws SQLException {
        try (final PreparedStatement ps = connection.prepareStatement(
                "select colName, typeName, \"LENGTH\", SCALE, \"DEFAULT\", \"NULLS\", REMARKS " +
                        "from SYSCAT.COLUMNS where tabSchema = ? and tabName = ? order by colNo")) {
            ps.setString(1, table.getSchema().getName());
            ps.setString(2, table.getName());
            try (final ResultSet resultSet = ps.executeQuery()) {
                final LinkedHashMap<String, Column> result = new LinkedHashMap<>();
                while (resultSet.next()) {
                    final Column column = new Column();
                    column.setTable(table);
                    column.setName(resultSet.getString("tabName"));
                    column.setNotNull( "N".equals(resultSet.getString("NULLS")) );
                    column.setDataType(readDataType(resultSet));
                    column.setComment(resultSet.getString("REMARKS"));
                    result.put(column.getName(), column);
                }
                return result;
            }
        }
    }

    /**
     * Refer to https://www.ibm.com/docs/en/db2/9.7?topic=views-syscatindexes
     * and https://www.ibm.com/docs/en/db2/11.1?topic=views-syscatindexes
     * @param connection the DB Connection
     * @param table the Table
     * @return a Map with index name as key and Index as value
     * @throws SQLException if it failed to query DB2
     */
    @Override
    public Map<String, Index> readIndexes(final Connection connection, final Table table) throws SQLException {
        try (final PreparedStatement ps = connection.prepareStatement(
                "select indName, colNames, uniqueRule " +
                        "from SYSCAT.INDEXES where tabSchema = ? and tabName = ? order by indName")) {
            ps.setString(1, table.getSchema().getName());
            ps.setString(2, table.getName());
            try (final ResultSet resultSet = ps.executeQuery()) {
                final Map<String, Index> result = new LinkedHashMap<>();
                while (resultSet.next()) {
                    final Index index = new Index();
                    index.setTable(table);
                    index.setName(resultSet.getString("tabName"));
                    index.setIndexType( readIndexType(resultSet.getString("uniqueRule")) );
                    index.setColumns( this.readColumnsOfIndex(table, resultSet.getString("colNames")) );
                    result.put(index.getName(), index);
                }
                return result;
            }
        }
    }

    @Override
    public String quote(final String identifier) {
        return '"' + identifier + '"'; // TODO: complicated case with double quote in the identifier itself
    }

    private static DataType readDataType(final ResultSet resultSet) throws SQLException {
        final String typeName = resultSet.getString("typeName");
        if ("INTEGER".equals(typeName)) {
            return IntegerType.instance;
        } else if ("BIGINT".equals(typeName)) {
            return BigIntType.instance;

        } else if ("DECIMAL".equals(typeName)) {
            final DecimalType decimalType = new DecimalType();
            decimalType.setPrecision(resultSet.getInt("LENGTH"));
            decimalType.setScale(resultSet.getInt("SCALE"));
            return decimalType;

        } else if ("VARCHAR".equals(typeName) || "LONG VARCHAR".equals(typeName)) {
            final VarCharType varCharType = new VarCharType();
            varCharType.setLength(resultSet.getInt("LENGTH"));
            return varCharType;
        } else if ("CHARACTER".equals(typeName)) {
            final CharType charType = new CharType();
            charType.setLength(resultSet.getInt("LENGTH"));
            return charType;

        } else if ("TIMESTAMP".equals(typeName)) {
            final TimestampType timestampType = new TimestampType();
            timestampType.setPrecision(resultSet.getInt("SCALE"));
            return timestampType;
        } else if ("DATE".equals(typeName)) {
            return DateType.instance;
        } else if ("TIME".equals(typeName)) {
            final TimeType timeType = new TimeType();
            timeType.setPrecision(resultSet.getInt("SCALE"));
            return timeType;

        } else if ("CLOB".equals(typeName)) {
            return ClobType.instance;

        } else if ("BLOB".equals(typeName)) {
            return BlobType.instance;

        } else {
            final UnknownType unknownType = new UnknownType();
            unknownType.setName(typeName);
            return unknownType;
        }
    }

    private static Index.IndexType readIndexType(final String uniqueRule) {
        if ("P".equals(uniqueRule)) {
            return IndexType.PRIMARY_KEY;
        } else if ("U".equals(uniqueRule)) {
            return IndexType.UNIQUE_KEY;
        } else {
            return IndexType.NORMAL;
        }
    }

    private List<Column> readColumnsOfIndex(final Table table, final String columnNames) {
        final String[] columnArray = StringUtils.splitPreserveAllTokens(columnNames, '+');
        if (columnArray.length <= 1) {
            throw new IllegalArgumentException("Invalid colNames: " + columnNames +
                    "\nTable: " + this.tableName(table));
        }
        final List<Column> columnList = new ArrayList<>(columnArray.length - 1);
        for (int i = 1; i < columnArray.length; i += 1) {
            columnList.add(
                    table.getColumnMap().get(columnArray[i])
            );
        }
        return columnList;
    }
}
