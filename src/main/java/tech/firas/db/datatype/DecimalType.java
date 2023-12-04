package tech.firas.db.datatype;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import lombok.Getter;

public class DecimalType implements DataType {

    private static final long serialVersionUID = 1L;

    @Getter private int precision;
    @Getter private int scale;

    public void setPrecision(final int precision) {
        if (precision < 0) {
            throw new IllegalArgumentException("Precision is not expected to be negative, but is " + precision);
        }
        this.precision = precision;
    }

    public void setScale(final int scale) {
        if (scale < 0) {
            throw new IllegalArgumentException("Scale is not expected to be negative, but is " + scale);
        }
        this.scale = scale;
    }

    @Override
    public String toString() {
        return "DataType[Decimal(" + this.precision +
                ", " + this.scale + ")]";
    }

    @Override
    public Object getFromResultSet(final ResultSet resultSet, final String columnName) throws SQLException {
        return resultSet.getBigDecimal(columnName);
    }

    @Override
    public void setPreparedStatementParameter(final PreparedStatement preparedStatement,
            final int index, final Object value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.DECIMAL);
        } else if (value instanceof BigDecimal) {
            preparedStatement.setBigDecimal(index, (BigDecimal) value);
        } else if (value instanceof BigInteger) {
            preparedStatement.setBigDecimal(index, new BigDecimal((BigInteger) value));
        } else if (value instanceof Long || value instanceof Integer || value instanceof Short) {
            preparedStatement.setBigDecimal(index, new BigDecimal(
                    BigInteger.valueOf( ((Number) value).longValue() )
            ));
        } else {
            preparedStatement.setBigDecimal(index, new BigDecimal(value.toString()));
        }
    }
}
