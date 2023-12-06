/*
 * Copyright 2023
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
