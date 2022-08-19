package etl.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

public class Extract {

    public Extract(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    static SparkSession spark;
    static String name;
    static String sizeFactor;

    public Dataset<Row> extractFromDelta(){


        final String rootPath = "/tmp/delta-";
        final String path = rootPath + name;

        System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Import data from delta-" + name);
        System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Start reading data");
        long start = System.currentTimeMillis();
        Dataset<Row> df = spark.read().format("delta")
                .load(path);
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        if (elapsedTime < 1000){
            System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Elapsed time to read" + df.count() + " lines: " + elapsedTime + " millis");
        }
        else {
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
            System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Elapsed time to read: " + df.count() + " lines: " + elapsedTimeSeconds + " seconds");
        }
        return df;
    }
}
