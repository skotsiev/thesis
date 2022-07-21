import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class StreamExecution {
    static public void execute(SparkSession spark, String query) throws StreamingQueryException, TimeoutException {

        StructType schemaLineitem = new StructType()
                .add("L_ORDERKEY", DataTypes.IntegerType)
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

        final String lineitemFile = "/home/soslan/Desktop/data/0.1GB/lineitem*.csv";

        Dataset<Row> lineItemStream = spark.readStream()
                .option("header", false)
                .format("csv")
                .schema(schemaLineitem)
                .csv(lineitemFile);

        lineItemStream.createOrReplaceTempView("LINEITEM");

        String q = Queries.hashMap.get(query);

        Dataset<Row> result = spark.sql(q);
        System.out.println("q: = " + q);
        StreamingQuery streamingQuery = result
                .writeStream()
                .outputMode(OutputMode.Update())
                .format("console").start();

        streamingQuery.awaitTermination();
    }
}
