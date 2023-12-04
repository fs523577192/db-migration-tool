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

    @Override
    public String createStatementFor(final Schema schema) {
        return super.createStatementFor(schema, true);
    }

    @Override
    public List<String> createStatementsFor(final Table table) {
        return super.createStatementsFor(table, true);
    }
}
