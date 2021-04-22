package spark.entity.source;

import spark.entity.table.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class DataSourceHDFS extends DataSource {


    public DataSourceHDFS(SparkSession sparkSession) {
        super(sparkSession);
    }

    @Override
    public Dataset read(Table table) {
        return sparkSession
                .read()
                .schema(table.getSchema())
                .option("header", true)
                .option("sep", ";")
                .csv("hdfs://"+table.getQualifiedName())
                .toDF();
    }

    @Override
    public SourceTypeEnum getType() {
        return SourceTypeEnum.HDFS;
    }
}
