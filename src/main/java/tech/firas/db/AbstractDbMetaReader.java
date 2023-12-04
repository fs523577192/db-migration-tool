package tech.firas.db;

import tech.firas.db.vo.Table;

public abstract class AbstractDbMetaReader extends AbstractDbMetaUtil implements DbMetaReader {

    @Override
    public String selectAllSqlFor(final Table table) {
        final String tableName = this.tableName(table);
        return "SELECT " + String.join(", ", table.getColumnMap().keySet()) +
                " FROM " + tableName;
    }

    @Override
    public String selectByPrimaryKeySqlFor(final Table table) {
        return this.selectAllSqlFor(table) + ' ' + this.whereSqlForPrimaryKey(table);
    }
}
