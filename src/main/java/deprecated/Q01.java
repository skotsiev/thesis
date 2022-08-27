package deprecated;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static etl.common.Schemas.createSchema;

public class Q01
{
    static public void executeStream(SparkSession spark) throws TimeoutException, StreamingQueryException {

        final String lineitemFile = "/home/soslan/Desktop/data/100MB/lineitem*.tbl";

        Dataset<Row> lineItemStreamDF = spark.readStream()
                .option("header", false)
                .option("delimiter", "|")
                .format("csv")
                .schema(createSchema("lineitem"))
                .csv(lineitemFile);

        lineItemStreamDF.createOrReplaceTempView("FS_LINEITEM");

        Dataset<Row> result = lineItemStreamDF
                .withColumn("DISC_PRICE"
                            , expr("l_extendedprice * ( 1 - l_discount)"))
                .withColumn("CHARGE"
                        , expr("l_extendedprice * ( 1 - l_discount) * ( 1 - l_tax )"))
                .groupBy(
                    col("l_returnflag")
                    , col("l_linestatus"))
                .agg(
                      sum(col("l_quantity"))
                    , sum(col("DISC_PRICE"))
                    , sum(col("CHARGE"))
                    , avg(col("L_QUANTITY"))
                    , avg(col("L_EXTENDEDPRICE"))
                    , avg(col("L_DISCOUNT"))
                    , count(col("L_RETURNFLAG"))
                );

        StreamingQuery streamingQueryTest = result
                .writeStream()
                .outputMode(OutputMode.Update())
                .format("console")
                .start();
        System.out.println("test");
        streamingQueryTest.awaitTermination();


    }
}
