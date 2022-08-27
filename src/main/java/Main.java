import deprecated.NativeQueriesStream;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import pipelines.StreamingUpdateFile;
import pipelines.StreamingUpdateSocket;
import pipelines.common.Initializer;
import pipelines.delta.*;
import pipelines.spark.DataAnalyticsBatch;
import pipelines.spark.InitialDataImport;
import pipelines.spark.UpdateTables;

import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import static etl.common.Constants.sizeFactorList;


public class Main {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        SparkSession spark = SparkSession
                .builder()
                .appName("spark-etl")
                .config("spark.master", "local")
                .config("spark.sql.shuffle.partitions", "5")
//                .config("spark.executor.memory", "16g")
                .config("spark.executor.instances", 4)
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        String execType = args[0].toLowerCase();
        String query = args[1].toLowerCase();
        System.out.println("[" + Main.class.getSimpleName() + "]\t\t\t" + "Executing with args: " + execType + ", " + query);

        ArrayList<String> sizeFactorList = sizeFactorList();;

        switch (execType) {
            case "extract": {
                for (String i : sizeFactorList) {
                    InitialDataImport pipeline = new InitialDataImport(spark, query, i);
                    pipeline.executePipeline();
                }
                for (String i : sizeFactorList) {
                    InitDataImportDelta pipeline = new InitDataImportDelta(spark, query, i);
                    pipeline.executePipeline();
                }
                break;
            }
            case "delta": {
                for (String i : sizeFactorList) {
                    InitDataImportDelta pipeline = new InitDataImportDelta(spark, query, i);
                    pipeline.executePipeline();
                }
                break;
            }
//            case "extract": {
//                InitialDataImport pipeline = new InitialDataImport(spark, query, "100MB");
//                pipeline.executePipeline();
//                break;
//            }
//            case "delta": {
//                InitDataImportDelta pipeline = new InitDataImportDelta(spark, query, "100MB");
//                pipeline.executePipeline();
//                break;
//            }
            case "streamupdate": {

                StreamingUpdateSocket pipeline = new StreamingUpdateSocket(spark, query);
                pipeline.executePipeline();
                break;
            }
            case "streamupdatecsv": {

                StreamingUpdateFile pipeline = new StreamingUpdateFile(spark, query);
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
            case "deltaappend": {

                UpdateTablesDelta pipeline = new UpdateTablesDelta(spark, query, "100MB");
                pipeline.executePipeline();
                break;
            }
            case "update": {

                UpdateTables pipeline = new UpdateTables(spark, query, "100MB");
                pipeline.executePipeline();
                break;
            }
            case "nativebatchdelta": {
                Initializer.initDelta(spark, "100MB");
                DataAnalyticsDelta pipeline = new DataAnalyticsDelta(spark);
                pipeline.executePipeline(query);
                break;
            }
            case "nativebatch": {
                Initializer.initJdbc(spark, "100MB");
                DataAnalyticsBatch pipeline = new DataAnalyticsBatch(spark);
                pipeline.executePipeline(query);
                break;
            }
            case "nativestream":
                Initializer.initJdbc(spark, "100MB");
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