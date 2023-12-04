package tech.firas.db.datatype;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class BlobType implements DataType {

    private static final long serialVersionUID = 1L;

    public static final BlobType instance = new BlobType();

    private BlobType() {
        if (instance != null) {
            throw new IllegalStateException("Illegal access");
        }
    }

    @Override
    public String toString() {
        return "DataType[Blob]";
    }

    @Override
    public Object getFromResultSet(final ResultSet resultSet, final String columnName) throws SQLException {
        return resultSet.getBlob(columnName);
    }

    @Override
    public void setPreparedStatementParameter(final PreparedStatement preparedStatement,
            final int index, final Object value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.BLOB);
        } else {
            preparedStatement.setBlob(index, (Blob) value);
        }
    }
}
