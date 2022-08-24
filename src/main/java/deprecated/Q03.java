package deprecated;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static etl.common.Constants.MYSQL_URL;
import static etl.common.Constants.connectionProperties;
import static org.apache.spark.sql.functions.*;
import static etl.common.Schemas.createSchema;

public class Q03 {
    static public void executeBatch(SparkSession spark) {
        Dataset<Row> customerDF = spark.read().jdbc(MYSQL_URL, "tpch.CUSTOMER", connectionProperties());
        Dataset<Row> lineItemDF = spark.read().jdbc(MYSQL_URL, "tpch.LINEITEM", connectionProperties());
        Dataset<Row> ordersDF = spark.read().jdbc(MYSQL_URL, "tpch.ORDERS", connectionProperties());

        Dataset<Row> fCustomerDF = customerDF.where(col("c_mktsegment").$eq$eq$eq("AUTOMOBILE"));
        Dataset<Row> fOrdersDF = ordersDF.where(col("o_orderdate").$eq$eq$eq("1995-03-15"));
        Dataset<Row> fLineItemDF = lineItemDF.where(col("l_shipdate").$greater("1995-03-15"));

        Dataset<Row> q03BatchResult = fCustomerDF
                .join(fOrdersDF
                        , col("c_custkey").$eq$eq$eq(col("o_custkey")))
                .join(fLineItemDF
                        , col("o_orderkey").$eq$eq$eq(col("l_orderkey")))
                .withColumn("volume"
                        , expr("l_extendedprice * ( 1 - l_discount)"))
                .groupBy(col("l_orderkey")
                        , col("o_orderdate")
                        , col("o_shippriority"))
                .agg(sum(col("volume"))
                        .as("revenue"))
                .sort(col("revenue").desc()
                        , col("o_orderdate"));

        q03BatchResult.show();
    }

    static public void executeStream(SparkSession spark) throws TimeoutException, StreamingQueryException {

        Dataset<Row> customerDF = spark.read().jdbc(MYSQL_URL, "tpch.CUSTOMER", connectionProperties());
        Dataset<Row> ordersDF = spark.read().jdbc(MYSQL_URL, "tpch.ORDERS", connectionProperties());

        final String lineitemFile = "/home/soslan/Desktop/data/0.1GB/lineitem*.tbl";
        final String ordersFile = "/home/soslan/Desktop/data/0.1GB/orders*.tbl";
        Dataset<Row> fOrdersDF = ordersDF.where(col("o_orderdate").$eq$eq$eq("1995-03-15"));

        Dataset<Row> lineItemStreamDF = spark.readStream()
                .option("header", false)
                .option("delimiter", "|")
                .format("csv")
                .schema(createSchema("lineitem"))
                .csv(lineitemFile)
                .where(col("l_shipdate").$greater("1995-03-15"));

        Dataset<Row> ordersStreamDF = spark.readStream()
                .option("header", false)
                .option("delimiter", "|")
                .format("csv")
                .schema(createSchema("orders"))
                .csv(ordersFile)
                .where(col("o_orderdate").$less("1995-03-15"));

        Dataset<Row> fCustomerDF = customerDF.where(col("c_mktsegment").$eq$eq$eq("AUTOMOBILE"));

        lineItemStreamDF.createOrReplaceTempView("FS_LINEITEM");
        ordersStreamDF.createOrReplaceTempView("FS_ORDERS");

        Dataset<Row> result = lineItemStreamDF
                .join(fOrdersDF
                        , col("l_orderkey").$eq$eq$eq(col("o_orderkey")))
                .join(fCustomerDF
                        ,col("o_custkey").$eq$eq$eq(col("c_custkey")))
                .withColumn("timestamp", current_timestamp())
//                .withWatermark("timestamp", "30 minutes")
                .withColumn("volume"
                        , expr("l_extendedprice * ( 1 - l_discount)"))
                .groupBy(col("l_orderkey")
                        , col("o_orderdate")
                        , col("o_shippriority")
                ,col("c_custkey"))
                .agg(sum(col("volume"))
                        .as("revenue"));

                StreamingQuery streamingQuery = result
                .writeStream()
                .outputMode(OutputMode.Update())
                .format("console")
                .start();
                streamingQuery.awaitTermination();
    }
}
