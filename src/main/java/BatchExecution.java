import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

public class BatchExecution {
    static public void execute(SparkSession spark, String query){
        long start = System.currentTimeMillis();
        Initializer.createViews(spark);
        long startQuery = System.currentTimeMillis();
        Dataset<Row> q = spark.sql(Queries.hashMap.get(query));
        long endQuery = System.currentTimeMillis();

        q.show();
        System.out.println("q2 count: = " + q.count());

        long startWrite = System.currentTimeMillis();
        q.write()
                .option("drop", "true")
                .mode("overwrite")
                .jdbc("jdbc:mysql://localhost:3306", "tpch.Q", Initializer.connectionProperties());
        long endWrite = System.currentTimeMillis();

        long end = System.currentTimeMillis();
        long elapsedTimeQuery = endQuery - startQuery;
        long elapsedTimeWrite = TimeUnit.MILLISECONDS.toSeconds(endWrite - startWrite);
        long elapsedTime = TimeUnit.MILLISECONDS.toSeconds(end - start);
        System.out.println("elapsedTimeQuery: = " + elapsedTimeQuery + " millis");
        System.out.println("elapsedTimeWrite: = " + elapsedTimeWrite + " sec");
        System.out.println("elapsedTime : = " + elapsedTime + " sec");
    }
}
