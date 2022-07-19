import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class Main {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        SparkSession spark = SparkSession.builder().appName("spark-etl").config("spark.master", "local").getOrCreate();

        Initializer.createViews(spark);
        long startQuery = System.currentTimeMillis();
        Dataset<Row> q2 = spark.sql(Queries.q2);
        long endQuery = System.currentTimeMillis();

        q2.show();
        System.out.println("q2 count: = " + q2.count());

        long startWrite = System.currentTimeMillis();
        q2.write()
                .option("truncate", "true")
                .mode("overwrite")
                .jdbc("jdbc:mysql://localhost:3306", "tpch.Q2", Initializer.connectionProperties());
        long endWrite = System.currentTimeMillis();

        long end = System.currentTimeMillis();
        long elapsedTimeQuery = endQuery - startQuery;
        long elapsedTimeWrite = TimeUnit.MILLISECONDS.toSeconds(endWrite - startWrite);
        long elapsedTime = TimeUnit.MILLISECONDS.toSeconds(end - start);
        System.out.println("elapsedTimeQuery: = " + elapsedTimeQuery + " millis");
        System.out.println("elapsedTimeWrite: = " + elapsedTimeWrite + " sec");
        System.out.println("elapsedTime : = " + elapsedTime + " sec");
        spark.stop();
    }
}