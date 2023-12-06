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

import java.sql.Time;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;

import lombok.Getter;

public class TimeType implements DataType {

    private static final long serialVersionUID = 1L;

    @Getter
    private int precision;

    public void setPrecision(final int precision) {
        if (precision < 0) {
            throw new IllegalArgumentException("Precision is not expected to be negative, but is " + precision);
        }
        this.precision = precision;
    }

    @Override
    public String toString() {
        return "DataType[Time(" + this.precision + ")]";
    }

    @Override
    public Object getFromResultSet(final ResultSet resultSet, final String columnName) throws SQLException {
        return resultSet.getTime(columnName);
    }

    @Override
    public void setPreparedStatementParameter(final PreparedStatement preparedStatement,
            final int index, final Object value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.TIME);
        } else if (value instanceof Time) {
            preparedStatement.setTime(index, (Time) value);
        } else if (value instanceof java.util.Date) {
            preparedStatement.setTime(index, new Time(
                    ((java.util.Date) value).getTime()
            ));
        } else if (value instanceof LocalTime) {
            preparedStatement.setTime(index, Time.valueOf((LocalTime) value));
        }
    }
}
