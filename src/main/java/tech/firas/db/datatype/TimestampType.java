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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
public class TimestampType implements DataType {

    private static final long serialVersionUID = 1L;

    @Getter private int precision;

    public void setPrecision(final int precision) {
        if (precision < 0) {
            throw new IllegalArgumentException("Precision is not expected to be negative, but is " + precision);
        }
        this.precision = precision;
    }

    @Override
    public String toString() {
        return "DataType[Timestamp(" + this.precision + ")]";
    }

    @Override
    public Object getFromResultSet(final ResultSet resultSet, final String columnName) throws SQLException {
        return resultSet.getTimestamp(columnName);
    }

    @Override
    public void setPreparedStatementParameter(final PreparedStatement preparedStatement,
            final int index, final Object value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(index, Types.TIMESTAMP);
        } else if (value instanceof Timestamp) {
            preparedStatement.setTimestamp(index, (Timestamp) value);
        } else if (value instanceof java.util.Date) {
            preparedStatement.setTimestamp(index, new Timestamp(
                    ((java.util.Date) value).getTime()
            ));
        } else if (value instanceof Instant) {
            preparedStatement.setTimestamp(index, Timestamp.from((Instant) value));
        } else if (value instanceof LocalDateTime) {
            preparedStatement.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
        } else if (value instanceof OffsetDateTime) {
            preparedStatement.setTimestamp(index, Timestamp.from(
                    ((OffsetDateTime) value).toInstant()
            ));
        } else if (value instanceof ZonedDateTime) {
            preparedStatement.setTimestamp(index, Timestamp.from(
                    ((ZonedDateTime) value).toInstant()
            ));
        }
    }
}
