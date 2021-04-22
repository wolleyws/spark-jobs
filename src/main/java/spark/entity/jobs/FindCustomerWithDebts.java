package spark.entity.jobs;

import spark.entity.source.DataSourceCSV;
import spark.entity.table.CustomerTable;
import spark.entity.table.DebtTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class FindCustomerWithDebts  implements Job {

    @Override
    public void execute(SparkSession sparkSession) {
        CustomerTable customerTable = new CustomerTable( "customers.csv", "data");
        Dataset customersDF = new DataSourceCSV(sparkSession).read(customerTable);
        customersDF.createOrReplaceTempView("customer");

        DebtTable debtTable = new DebtTable("debt.csv", "data");
        Dataset debtsDF = new DataSourceCSV(sparkSession).read(debtTable);
        debtsDF.createOrReplaceTempView("debt");

        //sql statement
        String sql = "SELECT customer.id, customer.name, debt.total FROM customer " +
                "INNER JOIN debt " +
                "ON customer.id == debt.customer_id";

        Dataset customerWithDebts = sparkSession.sql(sql);

        customerWithDebts.show();

        customerWithDebts
                .write()
                .option("header", true)
                .option("sep", ";")
                .csv("data/customers_with_debts");

    }
}
