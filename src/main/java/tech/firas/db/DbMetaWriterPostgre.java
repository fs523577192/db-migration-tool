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

package tech.firas.db;

import java.util.List;

import tech.firas.db.datatype.BlobType;
import tech.firas.db.datatype.ClobType;
import tech.firas.db.datatype.DataType;
import tech.firas.db.datatype.DecimalType;
import tech.firas.db.datatype.DoubleType;
import tech.firas.db.datatype.FloatType;
import tech.firas.db.vo.Schema;
import tech.firas.db.vo.Table;

public class DbMetaWriterPostgre extends AbstractDbMetaWriter {

    /**
     * Refer to <a href="https://www.postgresql.org/docs/11/datatype.html">data type</a>
     * @param dataType DataType
     * @return String
     */
    @Override
    public String dataTypeToString(final DataType dataType) {
        if (dataType instanceof DoubleType) {
            return "DOUBLE";
        } else if (dataType instanceof FloatType) {
            return "REAL";

        } else if (dataType instanceof ClobType) {
            return "TEXT";

        } else if (dataType instanceof BlobType) {
            return "BYTEA";

        } else if (dataType instanceof DecimalType) {
            final DecimalType decimalType = (DecimalType) dataType;
            return "NUMERIC" + (decimalType.getPrecision() > 0 ?
                    "(" + decimalType.getPrecision() +
                            (0 <= decimalType.getScale() && decimalType.getScale() <= decimalType.getPrecision() ?
                                    ", " + decimalType.getScale() : "") + ')' : "");

        } else {
            return super.dataTypeToString(dataType);
        }
    }

    /**
     * Quote the identifier
     * @param identifier the identifier to be quoted
     * @return the quoted identifier for PostgreSQL (quoted with double quote '"')
     */
    @Override
    public String quote(final String identifier) {
        return '"' + identifier + '"'; // TODO: complicated case with double quote in the identifier itself
    }

    @Override
    public String createStatementFor(final Schema schema) {
        return super.createStatementFor(schema, true);
    }

    @Override
    public List<String> createStatementsFor(final Table table) {
        return super.createStatementsFor(table, true);
    }
}
