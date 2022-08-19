package etl.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

public class LoadDelta {

    public LoadDelta(String name) {
        this.name = name;
    }

    private final String name;

    public void overwriteToDelta(SparkSession spark, Dataset<Row> data) {
        System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "overwriteToDelta: " + name);
        long start = System.currentTimeMillis();

        data.write()
                .format("delta")
                .mode("overwrite")
                .save("/tmp/delta-" + name);

        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        if (elapsedTime < 1000) {
            System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "Elapsed time to write: " + elapsedTime + " millis");
        } else {
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
            System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "Elapsed time to write: " + elapsedTimeSeconds + " seconds");
        }

        System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "delta-" + name);
        Dataset<Row> df = spark
                .read()
                .format("delta")
                .load("/tmp/delta-" + name);
        df.show();
    }

    public void appendToDelta(Dataset<Row> data, Boolean flag) {
        String path = "/tmp/delta-";
        if (flag) {
            path += name;
        } else {
            path += name + "-rejected";
        }
        System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "appendToDelta: " + path);
        System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" +  "data count: "+ data.count() );
        long start = System.currentTimeMillis();

        data.write()
                .format("delta")
                .mode("Append")
                .save(path);


        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        if (elapsedTime < 1000){
            System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "Elapsed time to write: " + elapsedTime + " millis");
        }
        else {
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
            System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "Elapsed time to write: " + elapsedTimeSeconds + " seconds");
        }
    }
}
