package deprecated;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import pipelines.common.Queries;

import java.util.concurrent.TimeUnit;

import static etl.common.Constants.MYSQL_URL;
import static etl.common.Constants.connectionProperties;

public class NativeQueriesBatch {
    static public void execute(SparkSession spark, String query){
        long start = System.currentTimeMillis();
        Dataset<Row> q = spark.sql(Queries.tpchQueries.get(query));
        long endQuery = System.currentTimeMillis();

        q.show();
        System.out.println(query + " count: = " + q.count());

        long startWrite = System.currentTimeMillis();
        q.write()
                .option("drop", "true")
                .mode("overwrite")
                .jdbc(MYSQL_URL, "data_analytics." + query, connectionProperties());
        long end = System.currentTimeMillis();
        long elapsedTimeQuery = endQuery - start;
        long elapsedTimeWrite = end - startWrite;
        long elapsedTime = end - start;

        if (elapsedTimeQuery < 1000){
            System.out.println("Elapsed time to query: " + elapsedTimeQuery + " millis");
        }
        else {
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
            System.out.println("Elapsed time to query: " + elapsedTimeSeconds + " seconds");
        }
        if (elapsedTimeWrite < 1000){
            System.out.println("Elapsed time to write: " + elapsedTimeWrite + " millis");
        }
        else {
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTimeWrite);
            System.out.println("Elapsed time to write: " + elapsedTimeSeconds + " seconds");
        }
        if (elapsedTime < 1000){
            System.out.println("Elapsed time to write: " + elapsedTime + " millis");
        }
        else {
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
            System.out.println("Elapsed time to write: " + elapsedTimeSeconds + " seconds");
        }
    }
}