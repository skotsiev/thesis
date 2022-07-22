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
                .add("L_PARTKEY", DataTypes.IntegerType)
                .add("L_SUPPKEY", DataTypes.IntegerType)
                .add("L_LINENUMBER", DataTypes.IntegerType)
                .add("L_QUANTITY", DataTypes.IntegerType)
                .add("L_EXTENDEDPRICE", DataTypes.DoubleType)
                .add("L_DISCOUNT", DataTypes.DoubleType)
                .add("L_TAX", DataTypes.DoubleType)
                .add("L_RETURNFLAG", DataTypes.StringType)
                .add("L_LINESTATUS", DataTypes.StringType)
                .add("L_SHIPDATE", DataTypes.StringType)
                .add("L_COMMITDATE", DataTypes.StringType)
                .add("L_RECEIPTDATE", DataTypes.StringType)
                .add("L_SHIPINSTRUCT", DataTypes.StringType)
                .add("L_SHIPMODE", DataTypes.StringType)
                .add("L_COMMENT", DataTypes.StringType);

        final String lineitemFile = "/home/soslan/Desktop/data/0.1GB/lineitem*.csv";

        Dataset<Row> lineItemStream = spark.readStream()
                .option("header", false)
                .option("delimiter","|")
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
