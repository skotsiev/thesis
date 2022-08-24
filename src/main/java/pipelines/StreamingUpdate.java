package pipelines;

import etl.common.Schemas;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class StreamingUpdate {

    public StreamingUpdate(SparkSession spark, String name) {
        this.spark = spark;
        this.name = name;
    }

    private final SparkSession spark;
    private final String name;

    public void executePipeline() throws TimeoutException, StreamingQueryException {
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();


        Dataset<Row> words = lines.select(
                split(col("value"),";").getItem(0).as("r_regionkey")
                ,split(col("value"),";").getItem(1).as("r_name")
                ,split(col("value"),";").getItem(2).as("r_comment"))
//                        split(col("value"),";").getItem(0).as("l_orderkey")
//                        ,split(col("value"),";").getItem(1).as("l_partkey")
//                        ,split(col("value"),";").getItem(2).as("l_suppkey")
//                        ,split(col("value"),";").getItem(3).as("l_linenumber")
//                        ,split(col("value"),";").getItem(4).as("l_quantity")
//                        ,split(col("value"),";").getItem(5).as("l_extendedprice")
//                        ,split(col("value"),";").getItem(6).as("l_discount")
//                        ,split(col("value"),";").getItem(7).as("l_tax")
//                        ,split(col("value"),";").getItem(8).as("l_returnflag")
//                        ,split(col("value"),";").getItem(9).as("l_linestatus")
//                        ,split(col("value"),";").getItem(10).as("l_shipdate")
//                        ,split(col("value"),";").getItem(11).as("l_commitdate")
//                        ,split(col("value"),";").getItem(12).as("l_receiptdate")
//                        ,split(col("value"),";").getItem(13).as("l_shipinstruct")
//                        ,split(col("value"),";").getItem(14).as("l_shipmode")
//                        ,split(col("value"),";").getItem(15).as("l_comment"))
                .drop("value");

//        Dataset<Row> wordCounts = words.groupBy("value").count();

//        StreamingQuery query = words.writeStream()
//                .outputMode("update")
//                .format("console")
//                .start();

        words.writeStream()
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", "/tmp/delta/_checkpoints/")
                .start("/tmp/delta-region");

        Dataset<Row> df = spark.readStream().format("delta").load("/tmp/delta-region");

        StreamingQuery query = df.writeStream()
                .outputMode("update")
                .format("console")
                .start();


    }
}
