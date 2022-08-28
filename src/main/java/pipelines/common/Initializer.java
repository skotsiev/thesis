package pipelines.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import pipelines.spark.DataAnalyticsBatch;

import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import static etl.common.Constants.*;
import static etl.common.Schemas.createSchema;
import static etl.common.Utils.elapsedTime;


public class Initializer {

    final static Logger logger = LogManager.getLogger(DataAnalyticsBatch.class);

    static public void initJdbc(SparkSession spark, String sizeFactor) {
        System.out.print("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "Initialization start...");
        logger.info("-----------------------------------------------------------------");
        logger.info("[" + Initializer.class.getSimpleName() + "]\t" + "sizeFactor " + sizeFactor);
        logger.info("-----------------------------------------------------------------");
        ArrayList<String> tableList = tableList();

        long startLoop = System.currentTimeMillis();
        for (String i : tableList) {
            String tableName = "warehouse" + sizeFactor + "." + i;
            Dataset<Row> dataFrame = spark.read().jdbc(MYSQL_URL, tableName, connectionProperties());
            dataFrame.createOrReplaceTempView(i);
        }
        long endLoop = System.currentTimeMillis();
        long elapsedTimeLoop = endLoop - startLoop;
        String elapsedTimeString = elapsedTime(elapsedTimeLoop);
        logger.info("[" + Initializer.class.getSimpleName() + "]\t" + "Total init time   :" + elapsedTimeString);
        logger.info("-----------------------------------------------------------------");
        System.out.println("Done");
    }

    static public void initDelta(SparkSession spark, String sizeFactor) {
        System.out.print("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "Initialization start...");
        ArrayList<String> tableList = tableList();
        logger.info("-----------------------------------------------------------------");
        logger.info("[" + Initializer.class.getSimpleName() + "]\t" + "sizeFactor " + sizeFactor);
        logger.info("-----------------------------------------------------------------");
        long startLoop = System.currentTimeMillis();

        for (String i : tableList) {
            String tablePath = "/tmp/delta-" + i + sizeFactor;
            Dataset<Row> deltaTable = spark.read().format("delta").load(tablePath);
            deltaTable.createOrReplaceTempView(i);
        }
        long endLoop = System.currentTimeMillis();
        long elapsedTimeLoop = endLoop - startLoop;
        String elapsedTimeString = elapsedTime(elapsedTimeLoop);
        logger.info("[" + Initializer.class.getSimpleName() + "]\t" + "Total init time   :" + elapsedTimeString);
        logger.info("-----------------------------------------------------------------");

        System.out.println("Done");
    }

    static public void initStream(SparkSession spark) throws TimeoutException, StreamingQueryException {
        System.out.print("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "Initialization start...");

        final String lineitemFile = "/home/soslan/Desktop/data/100MB/stream/lineitem*.csv";
        String path = "/tmp/delta-lineitem-stream";
        System.out.println("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "read...");

        Dataset<Row> lineItemStreamDF = spark.readStream()
                .option("header", false)
                .option("delimiter", "|")
                .format("csv")
                .schema(createSchema("lineitem"))
                .csv(lineitemFile);
        System.out.println("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "write...");

        StreamingQuery streamingQuery = lineItemStreamDF
                .writeStream()
//                .format("delta")
                .format("console")
                .outputMode("append")
                .start(path);

        streamingQuery.awaitTermination();
    }

}
