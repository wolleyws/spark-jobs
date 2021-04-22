package spark.entity.table;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CustomerTable extends Table {

    public CustomerTable() {
    }

    public CustomerTable(String fileName, String path) {
        super(fileName, path);
    }

    @Override
    public StructType getSchema() {
        return new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty())
        });
    }


}
