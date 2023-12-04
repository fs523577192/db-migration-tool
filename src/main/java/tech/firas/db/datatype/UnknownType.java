package tech.firas.db.datatype;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import lombok.Getter;
import lombok.Setter;

public class UnknownType implements DataType {

    private static final long serialVersionUID = 1L;

    @Getter @Setter private String name;

    @Override
    public String toString() {
        return "DataType[" + this.name + ']';
    }

    @Override
    public Object getFromResultSet(final ResultSet resultSet, final String columnName) throws SQLException {
        return resultSet.getString(columnName);
    }

    @Override
    public void setPreparedStatementParameter(final PreparedStatement preparedStatement,
            final int index, final Object value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.VARCHAR);
        } else {
            preparedStatement.setString(index, value.toString());
        }
    }
}