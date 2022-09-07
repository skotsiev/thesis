package pipelines.delta;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import pipelines.common.Initializer;
import pipelines.spark.DataAnalyticsBatch;

import static etl.common.Utils.elapsedTimeSeconds;


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

        InitDataImportDelta dataImport = new InitDataImportDelta(spark, "lineitem", sizeFactor);
        dataImport.executePipeline();

        Initializer.initDelta(spark, sizeFactor);
        DataAnalyticsDelta dataDelta = new DataAnalyticsDelta(spark, sizeFactor);
        dataDelta.executePipeline("q01");

        for (int i = 1; i <= fileCount; i ++){
            long startLoop = System.currentTimeMillis();
            UpdateTablesDelta2 updateLineItem = new UpdateTablesDelta2(spark, "lineitem", sizeFactor);
//            UpdateTablesDelta updateOrders = new UpdateTablesDelta(spark, "orders", sizeFactor);
            updateLineItem.executePipeline(i);
//            updateOrders.executePipeline(i);
            long endLoop = System.currentTimeMillis();
            long elapsedTimeLoop = endLoop - startLoop;
            String elapsedTimeLoopString = elapsedTimeSeconds(elapsedTimeLoop);
            logger.info("[" + getClass().getSimpleName() + "]\t" + "UpdateTablesDelta time: " + elapsedTimeLoopString);
            logger.info("=================================================================");

            Initializer.initDelta(spark, sizeFactor);
            dataDelta.executePipeline("q01");
            spark.read()
                    .format("delta")
                    .load("/tmp/delta-q011GB").show();
        }
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTimeSeconds(elapsedTime);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Benchmark elapsed time: " + elapsedTimeString);
        logger.info("=================================================================");
    }
}
