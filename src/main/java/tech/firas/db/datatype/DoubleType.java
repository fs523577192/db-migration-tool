package tech.firas.db.datatype;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class DoubleType implements DataType {

    private static final long serialVersionUID = 1L;

    public static final DoubleType instance = new DoubleType();

    private DoubleType() {
        if (instance != null) {
            throw new IllegalStateException("Illegal access");
        }
    }

    @Override
    public String toString() {
        return "DataType[Double]";
    }

    @Override
    public Object getFromResultSet(final ResultSet resultSet, final String columnName) throws SQLException {
        final double result = resultSet.getDouble(columnName);
        return resultSet.wasNull() ? null : result;
    }

    @Override
    public void setPreparedStatementParameter(final PreparedStatement preparedStatement,
            final int index, final Object value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.DOUBLE);
        } else {
            preparedStatement.setDouble(index, ((Number) value).doubleValue());
        }
    }
}
