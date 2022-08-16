import pipelines.DataAnalyticsBatch;
import pipelines.InitialDataImport;
import pipelines.UpdateTables;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import spark.deprecated.Q01;
import spark.deprecated.Q03;
import spark.common.Initializer;
import spark.stream.NativeQueriesStream;

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
        String execType = args[0].toLowerCase();
        String query = args[1].toLowerCase();
        System.out.println("[" + Main.class.getSimpleName() + "]\t\t\t" + "Executing with args: " + execType + ", " + query);

        if (execType.equals("extract")){

            InitialDataImport pipeline = new InitialDataImport(spark, query, "0.1GB");
            pipeline.executePipeline();
        }
        else if (execType.equals("update")){

            UpdateTables pipeline = new UpdateTables(spark, query, "0.1GB");
            pipeline.executePipeline();
        }
        else if (execType.equals("nativebatch")){
            Initializer.init(spark);
            DataAnalyticsBatch pipeline = new DataAnalyticsBatch(spark);
            pipeline.executePipeline(query);
        }
        else if (execType.equals("nativestream")) {
            Initializer.init(spark);
            NativeQueriesStream.execute(spark, query + "s");
        }
        else if (execType.equals("batch") && query.equals("q03")){
                Q03.executeBatch(spark);
        }
        else if (execType.equals("stream") && query.equals("q03")){
            Q03.executeStream(spark);
        }
        else if (execType.equals("stream") && query.equals("q01")){
            Q01.executeStream(spark);
        }
        else{
            System.out.println("[" + Main.class.getSimpleName() + "]\t" + "Invalid args");
        }
//        Thread.sleep(86400000);
        spark.stop();
    }
}