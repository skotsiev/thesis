package pipelines.delta;

import etl.delta.LoadDelta;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import pipelines.common.Queries;

import java.util.ArrayList;

import static etl.common.Constants.queriesList;
import static etl.common.Utils.elapsedTime;

public class DataAnalyticsDelta {

    final static Logger logger = LogManager.getLogger(DataAnalyticsDelta.class);

    public DataAnalyticsDelta(SparkSession spark, String sizeFactor) {
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
        ArrayList<Dataset<Row>> resultsDataFrame = new ArrayList<>();

        if (q.equals("all")) {
            long start = System.currentTimeMillis();
            ArrayList<String> queriesList = queriesList();

            for (String i : queriesList) {
                logger.info("[" + getClass().getSimpleName() + "]\t" + "Start query " + i );
                long startLoop = System.currentTimeMillis();
                execute(i);
                long endLoop = System.currentTimeMillis();
                long elapsedTimeLoop = endLoop - startLoop;
                String elapsedTimeString = elapsedTime(elapsedTimeLoop);
                System.out.println("[" + getClass().getSimpleName() + "]\t" + "Total process time: " + i + ":" + elapsedTimeString);
                logger.info("[" + getClass().getSimpleName() + "]\t" + "Total process time   :" + elapsedTimeString);
            }
            long end = System.currentTimeMillis();
            long elapsedTime = end - start;
            String elapsedTimeString = elapsedTime(elapsedTime);
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "Pipeline elapsed time: " + elapsedTimeString);
            logger.info("[" + getClass().getSimpleName() + "]\t" + "Pipeline elapsed time: " + elapsedTimeString);
        } else {
            execute(q);
        }
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Pipeline execution complete");
        logger.info("=================================================================");
    }

    private Dataset<Row> execute(String q) {
        String query = Queries.tpchQueries.get(q);

        Dataset<Row> queryResult = spark.sql(query);

        if (queryResult.count() != 0) {
            LoadDelta load = new LoadDelta(q);
            load.overwriteToDelta(spark, queryResult);
        }
        return queryResult;
    }
}