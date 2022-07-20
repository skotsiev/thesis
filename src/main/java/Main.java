import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("spark-etl").config("spark.master", "local").getOrCreate();

        if (args[0].equals("batch")){
            batchExecution.execute(spark, args[1]);
        }
        else if (args[0].equals("stream")){
            System.out.println("TBD");
        }
        else{
            System.out.println("invalid args");
        }

        spark.stop();
    }
}