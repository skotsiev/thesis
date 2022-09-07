package pipelines.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class ShowDeltaTableStream {

    public ShowDeltaTableStream(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name ;

    private final String sizeFactor;

    public void executePipeline() throws StreamingQueryException, TimeoutException {

        StreamingQuery streamingQuery = spark.readStream()
                .format("delta")
                .load("/tmp/delta-"+ name + sizeFactor)
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();
        streamingQuery.awaitTermination();
    }
}
