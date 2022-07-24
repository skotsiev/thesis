import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import spark.batch.NativeQueries;
import spark.Q03;
import spark.common.Initializer;
import spark.stream.StreamExecution;

import java.util.concurrent.TimeoutException;

public class Main {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        SparkSession spark = SparkSession
                .builder()
                .appName("spark-etl")
                .config("spark.master", "local")
                .config("spark.sql.shuffle.partitions", "5")
//                .config("spark.executor.memory", "16g")
//                .config("spark.executor.instances", 4)
                        .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        String execType = args[0];
        String query = args[1];
        System.out.println("Executing with args: " + execType + ", " + query);

        Initializer.initAll(spark);

        if (execType.equalsIgnoreCase("batch")){
            NativeQueries.execute(spark, query);
        }
        else if (execType.equalsIgnoreCase("stream")) {
            StreamExecution.execute(spark, query + "s");
        }
        else if (execType.equalsIgnoreCase("q03")){
                Q03.executeStream(spark);
        }
        else{
            System.out.println("invalid args");
        }

        spark.stop();
    }
}