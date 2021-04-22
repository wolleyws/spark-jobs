package spark.entity.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import spark.entity.source.DataSource;
import spark.entity.source.DataSourceHDFS;
import spark.entity.table.CustomerTable;

public class ExportCustomers implements Job{
    @Override
    public void execute(SparkSession sparkSession) {


        CustomerTable customerTable = new CustomerTable("customers.csv", "localhost:9000/data");
        DataSource dataSource = new DataSourceHDFS(sparkSession);
        Dataset dataset = dataSource.read(customerTable);

        dataset.write().parquet("data/customer_exported.parquet");

    }
}
