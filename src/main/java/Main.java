import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class Main {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession.builder().appName("spark-etl").config("spark.master", "local").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        if (args[0].equals("batch")){
            BatchExecution.execute(spark, args[1]);
        }
        else if (args[0].equals("stream")){
            StreamExecution.execute(spark, args[1]);
        }
        else{
            System.out.println("invalid args");
        }

        spark.stop();
    }
}