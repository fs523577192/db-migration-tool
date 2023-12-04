package tech.firas.db.datatype;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClobType implements DataType {

    private static final long serialVersionUID = 1L;

    public static final ClobType instance = new ClobType();

    private ClobType() {
        if (instance != null) {
            throw new IllegalStateException("Illegal access");
        }
    }

    @Override
    public String toString() {
        return "DataType[Clob]";
    }

    @Override
    public Object getFromResultSet(final ResultSet resultSet, final String columnName) throws SQLException {
        log.debug("Get CLOB as String: {}", columnName);
        return resultSet.getString(columnName);
        // use getString instead of getClob here
        // becuase PostgreSQL does not support getClob from TEXT column directly
    }

    @Override
    public void setPreparedStatementParameter(final PreparedStatement preparedStatement,
            final int index, final Object value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.CLOB);
        } else if (value instanceof Clob) {
            preparedStatement.setClob(index, (Clob) value);
        } else {
            preparedStatement.setString(index, value.toString());
        }
    }
}
