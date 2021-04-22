package spark.entity.source;

import spark.entity.table.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public abstract class DataSource {
    protected SparkSession sparkSession;

    public DataSource(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public abstract Dataset read(Table schema);

    public abstract SourceTypeEnum getType();
}
