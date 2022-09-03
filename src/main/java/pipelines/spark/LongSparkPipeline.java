package pipelines.spark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.spark.sql.SparkSession;
import pipelines.common.Initializer;

import static etl.common.Utils.elapsedTime;


public class LongSparkPipeline {

    final static Logger logger = LogManager.getLogger(DataAnalyticsBatch.class);

    public LongSparkPipeline(SparkSession spark, String sizeFactor, int fileCount) {
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

        InitialDataImport dataImport = new InitialDataImport(spark, "all", sizeFactor);
        dataImport.executePipeline();

        Initializer.initJdbc(spark, sizeFactor);
        DataAnalyticsBatch dataBatch = new DataAnalyticsBatch(spark, sizeFactor);
        dataBatch.executePipeline("all");

        for (int i = 1; i <= fileCount; i ++){
            UpdateTables updateLineItem = new UpdateTables(spark, "lineitem", sizeFactor);
            UpdateTables updateOrders = new UpdateTables(spark, "orders", sizeFactor);
            updateLineItem.executePipeline(i);
            updateOrders.executePipeline(i);
            Initializer.initJdbc(spark, sizeFactor);
            dataBatch.executePipeline("all");
        }
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTime(elapsedTime);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Benchmark elapsed time: " + elapsedTimeString);
        logger.info("=================================================================");

    }



}
