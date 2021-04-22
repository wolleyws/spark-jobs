package spark.entity.jobs;

import org.apache.spark.sql.SparkSession;

public interface Job {

    public void execute(SparkSession sparkSession);
}
