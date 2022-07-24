package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import spark.common.Initializer;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class Q03 {
    static public void executeBatch(SparkSession spark) {
        Dataset<Row> customerDF = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.CUSTOMER", Initializer.connectionProperties());
        Dataset<Row> lineItemDF = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.LINEITEM", Initializer.connectionProperties());
        Dataset<Row> ordersDF = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.ORDERS", Initializer.connectionProperties());

        Dataset<Row> fCustomerDF = customerDF.where(col("c_mktsegment").$eq$eq$eq("AUTOMOBILE"));
        Dataset<Row> fOrdersDF = ordersDF.where(col("o_orderdate").$less("1995-03-15"));
        Dataset<Row> fLineItemDF = lineItemDF.where(col("l_shipdate").$greater("1995-03-15"));

        Dataset<Row> q03BatchResult = fCustomerDF
                .join(fOrdersDF
                        , fCustomerDF.col("c_custkey").$eq$eq$eq(fOrdersDF.col("o_custkey")))
                .join(fLineItemDF
                        , fOrdersDF.col("o_orderkey").$eq$eq$eq(fLineItemDF.col("l_orderkey")))
                .withColumn("volume"
                        , expr("l_extendedprice * ( 1 - l_discount)"))
                .groupBy(fLineItemDF.col("l_orderkey")
                        , fOrdersDF.col("o_orderdate")
                        , fOrdersDF.col("o_shippriority"))
                .agg(sum(col("volume"))
                        .as("revenue"))
                .sort(col("revenue").desc(), fOrdersDF.col("o_orderdate"));

        q03BatchResult.show();
    }

    static public void executeStream(SparkSession spark) throws TimeoutException, StreamingQueryException {
        StructType schemaOrders = new StructType()
                .add("o_orderkey", DataTypes.IntegerType)
                .add("o_custkey", DataTypes.IntegerType)
                .add("o_orderstatus", DataTypes.StringType)
                .add("o_totalprice", DataTypes.DoubleType)
                .add("o_orderdate", DataTypes.StringType)
                .add("o_orderpriority", DataTypes.StringType)
                .add("o_clerk", DataTypes.IntegerType)
                .add("o_shippriority", DataTypes.IntegerType)
                .add("o_comment", DataTypes.StringType);

        StructType schemaLineitem = new StructType()
                .add("l_orderkey", DataTypes.IntegerType)
                .add("l_partkey", DataTypes.IntegerType)
                .add("l_suppkey", DataTypes.IntegerType)
                .add("l_linenumber", DataTypes.IntegerType)
                .add("l_quantity", DataTypes.IntegerType)
                .add("l_extendedprice", DataTypes.DoubleType)
                .add("l_discount", DataTypes.DoubleType)
                .add("l_tax", DataTypes.DoubleType)
                .add("l_returnflag", DataTypes.StringType)
                .add("l_linestatus", DataTypes.StringType)
                .add("l_shipdate", DataTypes.StringType)
                .add("l_commitdate", DataTypes.StringType)
                .add("l_receiptdate", DataTypes.StringType)
                .add("l_shipinstruct", DataTypes.StringType)
                .add("l_shipmode", DataTypes.StringType)
                .add("l_comment", DataTypes.StringType);

        Dataset<Row> customerDF = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.CUSTOMER", Initializer.connectionProperties());
        final String lineitemFile = "/home/soslan/Desktop/data/0.1GB/lineitem*.tbl";
        final String ordersFile = "/home/soslan/Desktop/data/0.1GB/orders*.tbl";


        Dataset<Row> lineItemStreamDF = spark.readStream()
                .option("header", false)
                .option("delimiter", "|")
                .format("csv")
                .schema(schemaLineitem)
                .csv(lineitemFile)
//                .withColumn("l_timestamp", current_timestamp() )
//                .withWatermark("l_timestamp", "10 minutes")
                .where(col("l_shipdate").$greater("1995-03-15"));

        Dataset<Row> ordersStreamDF = spark.readStream()
                .option("header", false)
                .option("delimiter", "|")
                .format("csv")
                .schema(schemaOrders)
                .csv(ordersFile)
//                .withColumn("o_timestamp", unix_timestamp())
//                .withWatermark("o_timestamp", "10 minutes")
                .where(col("o_orderdate").$less("1995-03-15"));

        Dataset<Row> fCustomerDF = customerDF.where(col("c_mktsegment").$eq$eq$eq("AUTOMOBILE"));

        Dataset<Row> q03StreamResult = fCustomerDF
                .withColumn("timestamp", current_timestamp())
                .withWatermark("timestamp", "10 minutes")
                .join(ordersStreamDF
                        , fCustomerDF.col("c_custkey").$eq$eq$eq(ordersStreamDF.col("o_custkey")))
                .join(lineItemStreamDF
                        , ordersStreamDF.col("o_orderkey").$eq$eq$eq(lineItemStreamDF.col("l_orderkey")))
                .withColumn("volume"
                        , expr("l_extendedprice * ( 1 - l_discount)"))
                .withColumn("timestamp", current_timestamp())
                .withWatermark("timestamp", "10 minutes")
                .groupBy(lineItemStreamDF.col("l_orderkey")
                        , ordersStreamDF.col("o_orderdate")
                        , ordersStreamDF.col("o_shippriority")
                        , col("timestamp")
                )
                .agg(sum(col("volume"))
                        .as("revenue"));

        StreamingQuery streamingQuery = q03StreamResult
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .start();

        streamingQuery.awaitTermination();
    }


}
