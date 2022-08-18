package pipelines;

import etl.Load;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.common.Queries;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class DataAnalyticsBatchDelta {

    public DataAnalyticsBatchDelta(SparkSession spark) {
        this.spark = spark;
    }

    private final SparkSession spark;

    public void executePipeline(String q){
        long start = System.currentTimeMillis();

        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executePipeline");
        if (q.equals("all")){
            ArrayList<String> queries = new ArrayList<>();
            queries.add("q01");
            queries.add("q02");
            queries.add("q03");
            queries.add("q04");
            queries.add("q05");
            queries.add("q06");
            queries.add("q07");
            queries.add("q08");
            queries.add("q09");
            queries.add("q10");
            queries.add("q11");
            queries.add("q12");
            queries.add("q13");
            queries.add("q14");
            queries.add("q16");
            queries.add("q17");
            queries.add("q18");
            queries.add("q19");
            queries.add("q20");
            queries.add("q21");
            queries.add("q22");

            for(String i : queries ) {
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
            Load load = new Load(q);
            load.overwriteToDelta(spark, queryResult);
        }
    }
}
