package pipelines.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static etl.common.Constants.ROOT_CSV_PATH;
import static etl.common.Schemas.createSchema;
import static org.apache.spark.sql.functions.*;

public class StreamingUpdateFile {

    public StreamingUpdateFile(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name;
    private final String sizeFactor;

    public void executePipeline() throws TimeoutException, StreamingQueryException {

        final String lineitemFile = ROOT_CSV_PATH +"/stream/";

        StreamingQuery streamingQuery = spark
                .readStream()
                .option("header", false)
                .option("delimiter", "|")
                .option("maxFilesPerTrigger", 1)
                .format("csv")
                .schema(createSchema("lineitem"))
                .csv(lineitemFile)
                .writeStream()
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", "/tmp/delta/_checkpoints/")
                .start("/tmp/delta-lineitem" + sizeFactor);


        System.out.println(streamingQuery.lastProgress());
        streamingQuery.awaitTermination();
    }
}
