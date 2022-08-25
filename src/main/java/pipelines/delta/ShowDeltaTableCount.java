package pipelines.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class ShowDeltaTableCount {

    public ShowDeltaTableCount(SparkSession spark, String name) {
        this.spark = spark;
        this.name = name;
    }

    private final SparkSession spark;
    private final String name ;

    public void executePipeline() throws StreamingQueryException, TimeoutException {

        Dataset<Row> dataFrameFromDelta = spark.readStream()
                .format("delta")
                .load("/tmp/delta-lineitem");

        Dataset<Row> result = dataFrameFromDelta.agg(count(col("l_orderkey")));


        StreamingQuery streamingQuery = result
                .writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        streamingQuery.awaitTermination();
    }
}
