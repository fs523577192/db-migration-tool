package tech.firas.db;

import tech.firas.db.vo.Table;

public interface DbMetaUtil {

    String tableName(Table table);

    String whereSqlForPrimaryKey(Table table);
}