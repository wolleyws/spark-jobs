package jobs;

import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import spark.entity.jobs.ExportCustomers;
import spark.entity.jobs.FindCustomerWithDebts;
import spark.entity.jobs.Job;

public class ExportCustomersTest {
    private SparkSession sparkSession;

    @Before
    public void startSession(){
        sparkSession = SparkSession
                .builder()
                .appName("Spark Jobs")
                .master("local")
                .getOrCreate();
    }

    @After
    public void stopSession(){
        sparkSession.stop();
    }


    @Test
    public void testJobFindCustomerWithDebts(){
        Job job = new ExportCustomers();
        job.execute(sparkSession);

    }
}
