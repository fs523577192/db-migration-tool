package tech.firas.db;

import tech.firas.db.datatype.BlobType;
import tech.firas.db.datatype.ClobType;
import tech.firas.db.datatype.DataType;
import tech.firas.db.datatype.DecimalType;
import tech.firas.db.datatype.DoubleType;
import tech.firas.db.datatype.FloatType;

/**
 * For "create schema", refer to
 * https://www.ibm.com/docs/en/db2/9.7?topic=statements-create-schema
 * and https://www.ibm.com/docs/en/db2/11.1?topic=statements-create-schema
 *
 * For "create table", refer to
 * https://www.ibm.com/docs/en/db2/9.7?topic=statements-create-table
 * and https://www.ibm.com/docs/en/db2/11.1?topic=statements-create-table
 *
 * For "create index", refer to
 * https://www.ibm.com/docs/en/db2/9.7?topic=statements-create-index
 * and https://www.ibm.com/docs/en/db2/11.1?topic=statements-create-index
 *
 * For "alter table", refer to
 * https://www.ibm.com/docs/en/db2/9.7?topic=statements-alter-table
 * and https://www.ibm.com/docs/en/db2/11.1?topic=statements-alter-table
 */
public class DbMetaWriterDB2 extends AbstractDbMetaWriter {

    /**
     * Refer to https://www.ibm.com/docs/en/db2/9.7?topic=elements-data-types
     * and https://www.ibm.com/docs/en/db2/11.1?topic=elements-data-types
     * @param dataType a DataType
     * @return the data type in SQL for DB2
     */
    @Override
    public String dataTypeToString(final DataType dataType) {
        if (dataType instanceof DoubleType) {
            return "DOUBLE PRECISION";
        } else if (dataType instanceof FloatType) {
            return "REAL";

        } else if (dataType instanceof ClobType) {
            return "CLOB";
        } else if (dataType instanceof BlobType) {
            return "BLOB";

        } else if (dataType instanceof DecimalType) {
            final DecimalType decimalType = (DecimalType) dataType;
            final StringBuilder stringBuilder = new StringBuilder("DECIMAL");
            if (decimalType.getPrecision() > 0) {
                stringBuilder.append('(').append(decimalType.getPrecision());
                if (0 <= decimalType.getScale() && decimalType.getScale() <= decimalType.getPrecision()) {
                    stringBuilder.append(", ").append(decimalType.getScale());
                }
                stringBuilder.append(')');
            }
            return stringBuilder.toString();

        } else {
            return super.dataTypeToString(dataType);
        }
    }
}
