package spark.batch;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.common.Initializer;
import spark.common.Queries;

import java.util.concurrent.TimeUnit;

public class NativeQueriesBatch {
    static public void execute(SparkSession spark, String query){
        long start = System.currentTimeMillis();
        Dataset<Row> q = spark.sql(Queries.hashMap.get(query));
        long endQuery = System.currentTimeMillis();

        q.show();
        System.out.println(query + " count: = " + q.count());

        long startWrite = System.currentTimeMillis();
        q.write()
                .option("drop", "true")
                .mode("overwrite")
                .jdbc("jdbc:mysql://localhost:3306", "tpch." + query, Initializer.connectionProperties());
        long end = System.currentTimeMillis();
        long elapsedTimeQuery = endQuery - start;
        long elapsedTimeWrite = TimeUnit.MILLISECONDS.toSeconds(end - startWrite);
        long elapsedTime = TimeUnit.MILLISECONDS.toSeconds(end - start);
        System.out.println("elapsedTimeQuery: = " + elapsedTimeQuery + " millis");
        System.out.println("elapsedTimeWrite: = " + elapsedTimeWrite + " sec");
        System.out.println("elapsedTime : = " + elapsedTime + " sec");
    }
}