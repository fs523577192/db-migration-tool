package tech.firas.db.datatype;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class SmallIntType implements DataType {

    private static final long serialVersionUID = 1L;

    public static final SmallIntType instance = new SmallIntType();

    private SmallIntType() {
        if (instance != null) {
            throw new IllegalStateException("Illegal access");
        }
    }

    @Override
    public String toString() {
        return "DataType[SmallInt]";
    }

    @Override
    public Object getFromResultSet(final ResultSet resultSet, final String columnName) throws SQLException {
        final short result = resultSet.getShort(columnName);
        return resultSet.wasNull() ? null : result;
    }

    @Override
    public void setPreparedStatementParameter(final PreparedStatement preparedStatement,
            final int index, final Object value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.INTEGER);
        } else {
            preparedStatement.setShort(index, ((Number) value).shortValue());
        }
    }
}
