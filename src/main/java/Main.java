import deprecated.NativeQueriesStream;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import pipelines.StreamingUpdate;
import pipelines.common.Initializer;
import pipelines.delta.*;
import pipelines.spark.DataAnalyticsBatch;
import pipelines.spark.InitialDataImport;
import pipelines.spark.UpdateTables;

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

        switch (execType) {
            case "extract": {

                InitialDataImport pipeline = new InitialDataImport(spark, query, "0.1GB");
                pipeline.executePipeline();
                break;
            }
            case "streamupdate": {

                StreamingUpdate pipeline = new StreamingUpdate(spark, query);
                pipeline.executePipeline();
                break;
            }
            case "deltastream": {

                ContinuousUpdate pipeline = new ContinuousUpdate(spark, query);
                pipeline.executePipeline();
                break;
            }
            case "showdelta": {

                ShowDeltaTable pipeline = new ShowDeltaTable(spark, query);
                pipeline.executePipeline();
                break;
            }
            case "showdeltacount": {

                ShowDeltaTableCount pipeline = new ShowDeltaTableCount(spark, query);
                pipeline.executePipeline();
                break;
            }
            case "delta": {

                InitialDataImportDelta pipeline = new InitialDataImportDelta(spark, query, "0.1GB");
                pipeline.executePipeline();
                break;
            }
            case "deltaappend": {

                UpdateTablesDelta pipeline = new UpdateTablesDelta(spark, query, "0.1GB");
                pipeline.executePipeline();
                break;
            }
            case "update": {

                UpdateTables pipeline = new UpdateTables(spark, query, "0.1GB");
                pipeline.executePipeline();
                break;
            }
            case "nativebatchdelta": {
                Initializer.initDelta(spark);
                DataAnalyticsBatchDelta pipeline = new DataAnalyticsBatchDelta(spark);
                pipeline.executePipeline(query);
                break;
            }
            case "nativebatch": {
                Initializer.initJdbc(spark);
                DataAnalyticsBatch pipeline = new DataAnalyticsBatch(spark);
                pipeline.executePipeline(query);
                break;
            }
            case "nativestream":
                Initializer.initJdbc(spark);
                NativeQueriesStream.execute(spark, query + "s");
                break;

            default:
                System.out.println("[" + Main.class.getSimpleName() + "]\t" + "Invalid args");
                break;
        }
//        Thread.sleep(86400000);
        spark.stop();
    }
}