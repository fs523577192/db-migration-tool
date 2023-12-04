package tech.firas.db.datatype;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class BigIntType implements DataType {

    private static final long serialVersionUID = 1L;

    public static final BigIntType instance = new BigIntType();

    private BigIntType() {
        if (instance != null) {
            throw new IllegalStateException("Illegal access");
        }
    }

    @Override
    public String toString() {
        return "DataType[BigInt]";
    }

    @Override
    public Object getFromResultSet(final ResultSet resultSet, final String columnName) throws SQLException {
        final long result = resultSet.getLong(columnName);
        return resultSet.wasNull() ? null : result;
    }

    @Override
    public void setPreparedStatementParameter(final PreparedStatement preparedStatement,
            final int index, final Object value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.INTEGER);
        } else {
            preparedStatement.setLong(index, ((Number) value).longValue());
        }
    }
}
