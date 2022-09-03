package pipelines.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

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

//        final String lineitemFile = "/home/soslan/Desktop/data/100MB/stream/lineitem*.csv";
        final String lineitemFile = ROOT_CSV_PATH +"/stream/";

        Dataset<Row> lineItemStreamDF = spark
                .readStream()
                .option("header", false)
                .option("delimiter", ";")
//                .option("rowsPerSecond", 3)
                .option("maxFilesPerTrigger", 1)
                .format("csv")
                .schema(createSchema("lineitem"))
                .csv(lineitemFile);

        lineItemStreamDF
                .writeStream()
                .format("delta")
//                .option("numRows", "3")
                .outputMode("append")
                .option("checkpointLocation", "/tmp/delta/_checkpoints/")
                .start("/tmp/delta-lineitem" + sizeFactor);

        Dataset<Row> dataFrameFromDelta = spark.readStream()
                .format("delta")
                .load("/tmp/delta-lineitem" + sizeFactor);

        Dataset<Row> result = dataFrameFromDelta.agg(count(col("l_orderkey")));

        StreamingQuery streamingQuery = result
                .writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        streamingQuery.awaitTermination();
    }
}
