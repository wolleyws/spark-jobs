package spark.entity.source;


import spark.entity.table.CustomerTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataSourceCSVTest {
    private  SparkSession session;

    @Before
    public void startSession(){
        session = SparkSession
                .builder()
                .appName("Spark Jobs")
                .master("local")
                .getOrCreate();
    }

    @After
    public void stopSession(){
        session.stop();
    }


    @Test
    public void testCreateDataSourceFromCSV() {
        String filePath = "src/test/java/resources/customers.csv";

        DataSource dataSourceCSV = new DataSourceCSV(session);
        Dataset dataset = dataSourceCSV.read(new CustomerTable());

        Assert.assertNotNull(dataset);
        Assert.assertEquals(4, dataset.count());

    }
}
