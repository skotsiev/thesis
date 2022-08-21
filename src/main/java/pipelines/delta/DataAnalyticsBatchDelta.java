package pipelines.delta;

import etl.delta.LoadDelta;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import pipelines.common.Queries;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static etl.common.Constants.queriesList;

public class DataAnalyticsBatchDelta {

    public DataAnalyticsBatchDelta(SparkSession spark) {
        this.spark = spark;
    }

    private final SparkSession spark;

    public void executePipeline(String q){
        long start = System.currentTimeMillis();

        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executePipeline");
        if (q.equals("all")){
            ArrayList<String> queriesList = queriesList();

            for(String i : queriesList ) {
                executeQuery(i);
            }
            long end = System.currentTimeMillis();
            long elapsedTimeQuery = end - start;
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toMinutes(elapsedTimeQuery);
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "Total elapsed time: " + elapsedTimeSeconds + " minutes");
        }
        else{
            executeQuery(q);
        }

    }

    private void executeQuery(String q){
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executeQuery: " + q);
        String query = Queries.hashMap.get(q);

        long start = System.currentTimeMillis();
        Dataset<Row> queryResult = spark.sql(query);
        long endQuery = System.currentTimeMillis();
        long elapsedTimeQuery = endQuery - start;

        if (elapsedTimeQuery < 1000){
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "Elapsed time to query: " + elapsedTimeQuery + " millis");
        }
        else {
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTimeQuery);
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "Elapsed time to query: " + elapsedTimeSeconds + " seconds");
        }

        System.out.println("[" + getClass().getSimpleName() + "]\t" + "queryResult.count(): " + queryResult.count() + " lines");

        
        if(queryResult.count() != 0){
            LoadDelta load = new LoadDelta(q);
            load.overwriteToDelta(spark, queryResult);
        }
    }
}
