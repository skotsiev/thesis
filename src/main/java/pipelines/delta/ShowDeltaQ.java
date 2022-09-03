package pipelines.delta;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.scheduler.StreamingListener;

import java.util.concurrent.TimeoutException;

public class ShowDeltaQ {


    public ShowDeltaQ(SparkSession spark, String name) {
        this.spark = spark;
        this.name = name;
    }

    private final SparkSession spark;
    private final String name;


    public void executePipeline() throws StreamingQueryException, TimeoutException {

        Dataset<Row> dataFrameFromDelta = spark.readStream()
                .format("delta")
                .load("/tmp/delta-" + name);

        StreamingQuery streamingQuery = dataFrameFromDelta
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();
        streamingQuery.awaitTermination();

    }
}
