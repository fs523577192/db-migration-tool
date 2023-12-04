package tech.firas.db.vo;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import tech.firas.db.datatype.DataType;
import tech.firas.db.Identifier;

@NoArgsConstructor
public class Column implements Serializable {

    private static final long serialVersionUID = 1L;

    @Getter @Setter private Table table;

    @Getter private String name;

    @Getter @Setter private DataType dataType;

    @Getter @Setter private boolean notNull;

    @Getter @Setter private String comment;

    public Column(final String name) {
        this.setName(name);
    }

    public void setName(final String name) {
        if (!Identifier.PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid column name: " + name);
        }
        this.name = name;
    }

    public Object getFromResultSet(final ResultSet resultSet) throws SQLException {
        return this.dataType.getFromResultSet(resultSet, this.name);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Column column = (Column) o;
        return Objects.equals(this.table, column.table) &&
                Objects.equals(this.name, column.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.table, this.name);
    }

    @Override
    public String toString() {
        return "Column{" +
                "table='" + this.table + '\'' +
                ", name='" + this.name + '\'' +
                ", dataType=" + this.dataType +
                ", notNull=" + this.notNull +
                '}';
    }
}
