package tech.firas.db;

import java.util.Objects;
import java.util.stream.Collectors;

import tech.firas.db.vo.Table;

public abstract class AbstractDbMetaUtil implements DbMetaUtil {

    @Override
    public String tableName(final Table table) {
        Objects.requireNonNull(table, "table must not be null");
        if (table.getSchema() == null) {
            return table.getName();
        }
        return table.getSchema().getName() + '.' + table.getName();
    }

    @Override
    public String whereSqlForPrimaryKey(final Table table) {
        Objects.requireNonNull(table, "table must not be null");
        return "WHERE " + table.getPrimaryKeyColumns().stream().map(column -> column.getName() + " = ?")
                .collect(Collectors.joining(" AND "));
    }
}
