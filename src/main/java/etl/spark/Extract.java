package etl.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

import static pipelines.common.Schemas.createSchema;

public class Extract {

    public Extract(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    static SparkSession spark;
    static String name;
    static String sizeFactor;



    public Dataset<Row> extractFromCsv(){
        final String rootPath = "/home/soslan/Desktop/data/";
        final String path = rootPath + sizeFactor + "/" + name + ".tbl";

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
