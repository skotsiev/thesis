package pipelines.delta;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.scheduler.*;

import java.util.concurrent.TimeoutException;

import static etl.common.Utils.elapsedTime;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class ShowDeltaTable implements StreamingListener {

    final static Logger logger = LogManager.getLogger(ShowDeltaTable.class);

    public ShowDeltaTable(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name ;
    private final String sizeFactor;
    private long start, end;

    public void executePipeline() throws StreamingQueryException, TimeoutException {

        Dataset<Row> dataFrameFromDelta = spark.readStream()
                .format("delta")
                .load("/tmp/delta-"+ name + sizeFactor);

        Dataset<Row> result = dataFrameFromDelta
                .withColumn("disc_price"
                        , expr("l_extendedprice * ( 1 - l_discount)"))
                .withColumn("charge"
                        , expr("l_extendedprice * ( 1 - l_discount) * ( 1 - l_tax )"))
                .groupBy(
                        col("l_returnflag")
                        , col("l_linestatus"))
                .agg(
                        sum(col("l_quantity"))
                        , sum(col("disc_price"))
                        , sum(col("charge"))
                        , avg(col("l_quantity"))
                        , avg(col("l_extendedprice"))
                        , avg(col("l_discount"))
                        , count(col("l_returnflag"))
                );

        StreamingQuery streamingQuery = result
                .writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        streamingQuery.awaitTermination();
    }

    @Override
    public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
        System.out.println("onBatchStarted 1");
        start = System.currentTimeMillis();
        System.out.println("onBatchStarted 2");
    }

    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
        System.out.println("onBatchCompleted 1");
        end = System.currentTimeMillis();
        long elapsedTime = end - start;
        System.out.println("onBatchCompleted 2");
        String elapsedTimeString = elapsedTime(elapsedTime);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Elapsed batch query time: " + elapsedTimeString);
        System.out.println("onBatchCompleted 3");
    }

    @Override
    public void onStreamingStarted(StreamingListenerStreamingStarted streamingStarted) {
        System.out.println("onStreamingStarted 1");

    }

    @Override
    public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
        System.out.println("onReceiverStarted 1");
    }

    @Override
    public void onReceiverError(StreamingListenerReceiverError receiverError) {
        System.out.println("onReceiverError 1");
    }

    @Override
    public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
        System.out.println("onReceiverStopped 1");
    }

    @Override
    public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
        System.out.println("onBatchSubmitted 1");
    }

    @Override
    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {
        System.out.println("onOutputOperationStarted 1");
    }

    @Override
    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {
        System.out.println("onOutputOperationCompleted 1");
    }
}
