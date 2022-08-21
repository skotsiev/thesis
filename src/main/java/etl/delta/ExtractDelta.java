package etl.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

import static etl.common.Constants.ROOT_CSV_PATH;
import static pipelines.common.Schemas.createSchema;

public class ExtractDelta {

    public ExtractDelta(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    SparkSession spark;
    String name;
    String sizeFactor;



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

    public Dataset<Row> extractFromCsv(Boolean updateFlag){
        final String rootPath = ROOT_CSV_PATH;
        final String path;
        if (updateFlag) path = rootPath + sizeFactor + "/update/" + name + ".tbl";
        else path = rootPath + sizeFactor + "/original/" + name + ".tbl";

        System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Import data from " + name + ".csv");
        System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Start reading data");
        long start = System.currentTimeMillis();
        Dataset<Row> dataFrame = spark.read()
                .option("header", false)
                .option("delimiter","|")
                .format("csv")
                .schema(createSchema(name))
                .csv(path);
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        if (elapsedTime < 1000){
            System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Elapsed time to read" + dataFrame.count() + " lines: " + elapsedTime + " millis");
        }
        else {
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
            System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Elapsed time to read: " + dataFrame.count() + " lines: " + elapsedTimeSeconds + " seconds");
        }
        return dataFrame;
    }

}
