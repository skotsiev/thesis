package pipelines.spark;

import etl.spark.LoadSpark;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import pipelines.common.Queries;

import java.util.ArrayList;

import static etl.common.Constants.queriesList;
import static etl.common.Utils.elapsedTimeSeconds;

public class DataAnalyticsBatch {

    final static Logger logger = LogManager.getLogger(DataAnalyticsBatch.class);

    public DataAnalyticsBatch(SparkSession spark, String sizeFactor) {
        this.spark = spark;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String sizeFactor;
    public void executePipeline(String q) {
        logger.info("=======================[" + getClass().getSimpleName() + "]======================");
        logger.info("=================================================================");
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Starting pipeline execution");
        logger.info("-----------------------------------------------------------------");
        logger.info("[" + getClass().getSimpleName() + "]\t" + "sizeFactor " + sizeFactor);
        logger.info("-----------------------------------------------------------------");
        long start = System.currentTimeMillis();

        if (q.equals("all")) {
            ArrayList<String> queriesList = queriesList();

            for (String query : queriesList) {
                long startLoop = System.currentTimeMillis();
                execute(query);
                long endLoop = System.currentTimeMillis();
                long elapsedTimeLoop = endLoop - startLoop;
                String elapsedTimeString = elapsedTimeSeconds(elapsedTimeLoop);
                logger.info("[" + getClass().getSimpleName() + "]\t" + "Time for " + query + ":" + elapsedTimeString);
            }
            long end = System.currentTimeMillis();
            long elapsedTime = end - start;
            String elapsedTimeString = elapsedTimeSeconds(elapsedTime);
            logger.info("[" + getClass().getSimpleName() + "]\t" + "Pipeline elapsed time: " + elapsedTimeString);
        } else {

            execute(q);
        }
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Pipeline execution complete");
        logger.info("=================================================================");
    }

    private void execute(String q) {
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executeQuery: " + q);
        String query = Queries.tpchQueries.get(q);

        Dataset<Row> queryResult = spark.sql(query);

        if (queryResult.count() != 0) {
            LoadSpark load = new LoadSpark(q, sizeFactor);
            load.overwriteToMysql(queryResult, "data_analytics" + sizeFactor);
        }
    }
}