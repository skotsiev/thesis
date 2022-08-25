package pipelines.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class ShowDeltaTable {

    public ShowDeltaTable(SparkSession spark, String name) {
        this.spark = spark;
        this.name = name;
    }

    private final SparkSession spark;
    private final String name ;

    public void executePipeline() throws StreamingQueryException, TimeoutException {

        Dataset<Row> dataFrameFromDelta = spark.readStream()
                .format("delta")
                .load("/tmp/delta-lineitem");

        Dataset<Row> result = dataFrameFromDelta
                .withColumn("disc_price"
                        , expr("l_extendedprice * ( 1 - l_discount)"))
                .withColumn("charge"
                        , expr("l_extendedprice * ( 1 - l_discount) * ( 1 - l_tax )"))
                .groupBy(
                        col("l_returnflag")
                        , col("l_linestatus"))
                .agg(
                        sum(col("l_quantity"))
                        , sum(col("disc_price"))
                        , sum(col("charge"))
                        , avg(col("l_quantity"))
                        , avg(col("l_extendedprice"))
                        , avg(col("l_discount"))
                        , count(col("l_returnflag"))
                );

        StreamingQuery streamingQuery = result
                .writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        streamingQuery.awaitTermination();
    }
}
