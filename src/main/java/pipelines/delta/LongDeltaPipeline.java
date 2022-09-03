package pipelines.delta;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import pipelines.common.Initializer;
import pipelines.spark.DataAnalyticsBatch;

import static etl.common.Utils.elapsedTime;


public class LongDeltaPipeline {

    final static Logger logger = LogManager.getLogger(DataAnalyticsBatch.class);

    public LongDeltaPipeline(SparkSession spark, String sizeFactor, int fileCount) {
        this.spark = spark;
        this.sizeFactor = sizeFactor;
        this.fileCount = fileCount;
    }

    private final SparkSession spark;
    private final String sizeFactor;

    private final int fileCount;

    public void executePipeline(){
        logger.info("=======================[" + getClass().getSimpleName() + "]======================");
        logger.info("=================================================================");
        long start = System.currentTimeMillis();

        InitDataImportDelta dataImport = new InitDataImportDelta(spark, "all", sizeFactor);
        dataImport.executePipeline();

        Initializer.initDelta(spark, sizeFactor);
        DataAnalyticsDelta dataDelta = new DataAnalyticsDelta(spark, sizeFactor);
        dataDelta.executePipeline("all");

        for (int i = 1; i <= fileCount; i ++){
            UpdateTablesDelta updateLineItem = new UpdateTablesDelta(spark, "lineitem", sizeFactor);
            UpdateTablesDelta updateOrders = new UpdateTablesDelta(spark, "orders", sizeFactor);
            updateLineItem.executePipeline(i);
            updateOrders.executePipeline(i);
            Initializer.initDelta(spark, sizeFactor);
            dataDelta.executePipeline("all");
            spark.read()
                    .format("delta")
                    .load("/tmp/delta-q011GB").show();
        }
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTime(elapsedTime);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Benchmark elapsed time: " + elapsedTimeString);
        logger.info("=================================================================");

    }



}
