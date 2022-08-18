package pipelines;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.*;

import java.util.concurrent.TimeoutException;

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


        Dataset<Row> words = lines
                .withColumn("split", split(col("value"), "|"))
//                .select(col("split")(0).as("1st_split"), col("split")(1).as("2nd_split"),col("split")(2).as("3rd_split"),col("split")(3).as("4th_split"));
                ;

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = words.writeStream()
                .outputMode("update")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
