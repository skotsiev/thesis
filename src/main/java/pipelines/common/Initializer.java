package pipelines.common;

import etl.common.Schemas;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import static etl.common.Constants.*;
import static etl.common.Schemas.createSchema;

public class Initializer {

    static public void initJdbc(SparkSession spark) {
        System.out.print("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "Initialization start...");
        ArrayList<String> tableList = tableList();

        for (String i : tableList) {
            String tableName = "warehouse." + i;
            Dataset<Row> dataFrame = spark.read().jdbc(MYSQL_URL, tableName, connectionProperties());
            dataFrame.createOrReplaceTempView(i);
        }
        System.out.println("Done");
    }

    static public void initDelta(SparkSession spark) {
        System.out.print("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "Initialization start...");
        ArrayList<String> tableList = tableList();

        for (String i : tableList) {
            String tablePath = "/tmp/delta-" + i;
            Dataset<Row> deltaTable = spark.read().format("delta").load(tablePath);
            deltaTable.createOrReplaceTempView(i);
        }
        System.out.println("Done");
    }

    static public void initStream(SparkSession spark) throws TimeoutException, StreamingQueryException {
        System.out.print("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "Initialization start...");

        final String lineitemFile = "/home/soslan/Desktop/data/0.1GB/stream/lineitem*.csv";
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
