package tech.firas.db.datatype;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface DataType extends Serializable {

    Object getFromResultSet(ResultSet resultSet, String columnName) throws SQLException;
    void setPreparedStatementParameter(PreparedStatement preparedStatement, int index, Object value) throws SQLException;
}
