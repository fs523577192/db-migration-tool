package tech.firas.db.datatype;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;

public class DateType implements DataType {

    private static final long serialVersionUID = 1L;

    public static final DateType instance = new DateType();

    private DateType() {
        if (instance != null) {
            throw new IllegalStateException("Illegal access");
        }
    }

    @Override
    public String toString() {
        return "DataType[Date]";
    }

    @Override
    public Object getFromResultSet(final ResultSet resultSet, final String columnName) throws SQLException {
        return resultSet.getDate(columnName);
    }

    @Override
    public void setPreparedStatementParameter(final PreparedStatement preparedStatement,
            final int index, final Object value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.DATE);
        } else if (value instanceof Date) {
            preparedStatement.setDate(index, (Date) value);
        } else if (value instanceof java.util.Date) {
            preparedStatement.setDate(index, new Date(
                    ((java.util.Date) value).getTime()
            ));
        } else if (value instanceof LocalDate) {
            preparedStatement.setDate(index, Date.valueOf((LocalDate) value));
        }
    }
}
