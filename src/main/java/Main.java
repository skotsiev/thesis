import deprecated.NativeQueriesStream;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import pipelines.delta.StreamingUpdateFile;
import pipelines.delta.StreamingUpdateSocket;
import pipelines.common.Initializer;
import pipelines.delta.*;
import pipelines.spark.DataAnalyticsBatch;
import pipelines.spark.InitialDataImport;
import pipelines.spark.LongSparkPipeline;
import pipelines.spark.UpdateTables;

import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import static etl.common.Constants.sizeFactorList;


public class Main {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        String execType = args[0].toLowerCase();
        String query = args[1].toLowerCase();

        SparkSession spark = SparkSession
                .builder()
                .appName(execType)
                .config("spark.master", "local")
                .config("spark.eventLog.enabled", "true")
                .config("spark.executor.memory", "8g")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("[" + Main.class.getSimpleName() + "]\t\t\t" + "Executing with args: " + execType + ", " + query);

        ArrayList<String> sizeFactorList = sizeFactorList();

        switch (execType) {
            case "initial-import-delta": {
                for (String sizeFactor : sizeFactorList) {
                    InitDataImportDelta pipeline = new InitDataImportDelta(spark, query, sizeFactor);
                    pipeline.executePipeline();
                }
                break;
            }
            case "initial-import-spark": {
                for (String i : sizeFactorList) {
                    InitialDataImport pipeline = new InitialDataImport(spark, query, i);
                    pipeline.executePipeline();
                }
                break;
            }
//            case "native-batch-spark": {
//                for (String sizeFactor : sizeFactorList) {
//                    Initializer.initJdbc(spark, sizeFactor);
//                    DataAnalyticsBatch pipeline = new DataAnalyticsBatch(spark, sizeFactor);
//                    pipeline.executePipeline(query);
//                }
//                break;
//            }
            case "native-batch-delta": {
                    Initializer.initDelta(spark, "100MB");
                    DataAnalyticsDelta pipeline = new DataAnalyticsDelta(spark, "100MB");
                    pipeline.executePipeline(query);
                break;
            }
//            case "native-batch-delta": {
//                for (String sizeFactor : sizeFactorList) {
//                    Initializer.initDelta(spark, sizeFactor);
//                    DataAnalyticsDelta pipeline = new DataAnalyticsDelta(spark, sizeFactor);
//                    pipeline.executePipeline(query);
//                }
//                break;
//            }
            case "update-data-spark": {
                for (String i : sizeFactorList) {
                    UpdateTables pipeline = new UpdateTables(spark, query, i);
                    pipeline.executePipeline();
                }
                break;
            }
            case "update-data-delta": {
                for (String i : sizeFactorList) {
                    UpdateTablesDelta2 pipeline = new UpdateTablesDelta2(spark, query, i);
                    pipeline.executePipeline();
                }
                break;
            }
            case "delta": {
                InitDataImportDelta pipeline = new InitDataImportDelta(spark, query, "1GB");
                pipeline.executePipeline();
                break;
            }
            case "spark": {
                InitialDataImport pipeline = new InitialDataImport(spark, query, "1GB");
                pipeline.executePipeline();
                break;
            }
            case "consecutive-updates-spark": {
                LongSparkPipeline pipeline = new LongSparkPipeline(spark, "1GB", 5);
                pipeline.executePipeline();
                break;
            }
            case "consecutive-updates-delta": {
                LongDeltaPipeline pipeline = new LongDeltaPipeline(spark, "1GB", 5);
                pipeline.executePipeline();
                break;
            }
            case "consecutive-updates-stream": {
                ShowDeltaTable pipeline = new ShowDeltaTable(spark, query, "1GB");
                pipeline.executePipeline();
                break;
            }
            case "native-batch-spark": {
                Initializer.initJdbc(spark, "5GB");
                DataAnalyticsBatch pipeline = new DataAnalyticsBatch(spark, "5GB");
                pipeline.executePipeline(query);
                break;
            }
            case "streamupdate": {
                StreamingUpdateSocket pipeline = new StreamingUpdateSocket(spark, query);
                pipeline.executePipeline();
                break;
            }
            case "stream-update-csv": {
                StreamingUpdateFile pipeline = new StreamingUpdateFile(spark, query,"1GB");
                pipeline.executePipeline();
                break;
            }
            case "deltastream": {
                ContinuousUpdate pipeline = new ContinuousUpdate(spark, query);
                pipeline.executePipeline();
                break;
            }

            case "showdelta": {
                ShowDeltaQ pipeline = new ShowDeltaQ(spark, query, "1GB");
                pipeline.executePipeline();
                break;
            }
            case "showdeltacount": {
                ShowDeltaTableCount pipeline = new ShowDeltaTableCount(spark, query);
                pipeline.executePipeline();
                break;
            }
            case "show-delta-stream": {
                ShowDeltaTableStream pipeline = new ShowDeltaTableStream(spark, query, "1GB");
                pipeline.executePipeline();
                break;
            }
            case "deltaappend": {
                UpdateTablesDelta pipeline = new UpdateTablesDelta(spark, query, "100MB");
                pipeline.executePipeline();
                break;
            }
            case "deltaappend2": {
                UpdateTablesDelta2 pipeline = new UpdateTablesDelta2(spark, query, "100MB");
                pipeline.executePipeline();
                break;
            }
//            case "update-data-spark": {
//                UpdateTables pipeline = new UpdateTables(spark, query, "100MB");
//                pipeline.executePipeline();
//                break;
//            }
            default:
                System.out.println("[" + Main.class.getSimpleName() + "]\t" + "Invalid args");
                break;
        }
//        Thread.sleep(86400000);
        spark.stop();
    }
}