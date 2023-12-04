package tech.firas.db.vo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import tech.firas.db.Identifier;

@NoArgsConstructor
public class Index implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum IndexType {
        PRIMARY_KEY,
        UNIQUE_KEY,
        NORMAL
    }

    @Getter @Setter private Table table;

    @Getter private String name;

    @Getter @Setter private IndexType indexType;

    @Getter @Setter private List<Column> columns;

    public void setName(final String name) {
        if (!Identifier.PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid index name: " + name);
        }
        this.name = name;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Index index = (Index) o;
        return Objects.equals(table, index.table) &&
                Objects.equals(name, index.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.table, this.name);
    }

    @Override
    public String toString() {
        String columnsToString = "null";
        final List<Column> columns = this.columns;
        if (columns != null) {
            final List<String> columnNames = new ArrayList<>(columns.size());
            for (final Column column : columns) {
                columnNames.add(column.getName());
            }
            columnsToString = columnNames.toString();
        }
        return "Index{" +
                "table='" + this.table + '\'' +
                ", name='" + this.name + '\'' +
                ", indexType=" + this.indexType +
                ", columns=" + columnsToString +
                '}';
    }
}
