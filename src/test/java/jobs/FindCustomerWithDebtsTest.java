package jobs;

import spark.entity.jobs.FindCustomerWithDebts;
import spark.entity.jobs.Job;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FindCustomerWithDebtsTest {
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
        Job job = new FindCustomerWithDebts();
        job.execute(sparkSession);

    }
}
