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


    public ShowDeltaQ(SparkSession spark, String name,  String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name;
    private final String sizeFactor;


    public void executePipeline() throws StreamingQueryException, TimeoutException {

        Dataset<Row> dataFrameFromDelta = spark.read()
                .format("delta")
                .load("/tmp/delta-" + name + sizeFactor);
        System.out.println("dataFrameFromDelta.count: " + dataFrameFromDelta.count());
        dataFrameFromDelta.show();
    }
}
