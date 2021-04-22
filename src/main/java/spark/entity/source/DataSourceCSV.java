package spark.entity.source;

import spark.entity.table.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class DataSourceCSV extends DataSource {
    public DataSourceCSV(SparkSession sparkSession) {
        super(sparkSession);
    }


    @Override
    public Dataset read(Table table) {
        return sparkSession
                .read()
                .schema(table.getSchema())
                .option("header", true)
                .option("sep", ";")
                .csv(table.getQualifiedName())
                .toDF();
    }

    @Override
    public SourceTypeEnum getType() {
        return SourceTypeEnum.CSV;
    }
}
