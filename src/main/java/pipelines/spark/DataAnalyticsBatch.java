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
import static etl.common.Utils.elapsedTime;

public class DataAnalyticsBatch {

    final static Logger logger = LogManager.getLogger(DataAnalyticsBatch.class);

    public DataAnalyticsBatch(SparkSession spark) {
        this.spark = spark;
    }

    private final SparkSession spark;

    public void executePipeline(String q) {
        logger.info("=======================[" + getClass().getSimpleName() + "]=======================");
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Starting pipeline execution");
        ArrayList<Dataset<Row>> resultsDataFrame = new ArrayList<>();

        if (q.equals("all")) {
            long start = System.currentTimeMillis();
            ArrayList<String> queriesList = queriesList();

            for (String i : queriesList) {
                resultsDataFrame.add(executeQuery(i));
            }
            long end = System.currentTimeMillis();
            long elapsedTime = end - start;
            String elapsedTimeString = elapsedTime(elapsedTime);

            logger.info("[" + getClass().getSimpleName() + "]\t" + "Total elapsed query time: " + elapsedTimeString);

            long startWrite = System.currentTimeMillis();
            for (String i : queriesList) {
                Dataset<Row> d = resultsDataFrame.get(queriesList.indexOf(i));
                if (d.count() != 0) {
                    LoadSpark load = new LoadSpark(i);
                    load.overwriteToMysql(d, "data_analytics");
                }
            }
            long endWrite = System.currentTimeMillis();
            long elapsedWriteTime = endWrite - startWrite;
            String elapsedWriteTimeString = elapsedTime(elapsedWriteTime);

            logger.info("[" + getClass().getSimpleName() + "]\t" + "Total elapsed write time: " + elapsedWriteTimeString);
            logger.info("[" + getClass().getSimpleName() + "]\t" + "Pipeline execution complete");
            logger.info("==================================================================");

        } else {
            LoadSpark load = new LoadSpark(q);
            Dataset<Row> d = executeQuery(q);
            load.overwriteToMysql(d, "data_analytics");
        }
    }

    private Dataset<Row> executeQuery(String q) {
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executeQuery: " + q);
        String query = Queries.hashMap.get(q);

        long start = System.currentTimeMillis();
        Dataset<Row> queryResult = spark.sql(query);
        long end = System.currentTimeMillis();

        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTime(elapsedTime);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Elapsed time to query " + q + ": " + elapsedTimeString);

        return queryResult;
    }
}