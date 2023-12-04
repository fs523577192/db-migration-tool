package tech.firas.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import tech.firas.db.vo.Column;
import tech.firas.db.vo.Index;
import tech.firas.db.vo.Schema;
import tech.firas.db.vo.Table;

public interface DbMetaReader extends DbMetaUtil {

    Set<Schema> read(Connection connection) throws SQLException;

    Set<Table> readTables(Connection connection, Schema schema) throws SQLException;

    Map<String, Column> readColumns(Connection connection, Table table) throws SQLException;
    Map<String, Index> readIndexes(Connection connection, Table table) throws SQLException;

    String selectAllSqlFor(Table table);
    String selectByPrimaryKeySqlFor(Table table);
}
