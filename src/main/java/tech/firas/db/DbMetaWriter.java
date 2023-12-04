package tech.firas.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import tech.firas.db.datatype.DataType;
import tech.firas.db.vo.Column;
import tech.firas.db.vo.Index;
import tech.firas.db.vo.Schema;
import tech.firas.db.vo.Table;

public interface DbMetaWriter extends DbMetaUtil {

    String dataTypeToString(DataType dataType);

    String columnInCreateTable(Column column);

    void createIndex(Connection connection, Index index) throws SQLException;
    String createStatementFor(Index index);

    void createTable(Connection connection, Table table) throws SQLException;
    List<String> createStatementsFor(Table table);

    void createSchema(Connection connection, Schema schema) throws SQLException;
    String createStatementFor(Schema schema);

    void createColumn(Connection connection, Column column) throws SQLException;
    String createStatementFor(Column column);

    String deleteAllSqlFor(Table table);
    String deleteByPrimaryKeySqlFor(Table table);

    String insertSqlFor(Table table);

    String updateByPrimaryKeySqlFor(Table table);

    String truncateTableSqlFor(Table table);
}
