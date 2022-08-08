package etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.concurrent.TimeUnit;

import static etl.Transform.validateDimensions;
import static spark.common.Schemas.createSchema;

public class Extract {
    static public void extractFromCsv(SparkSession spark, String file, String sizeFactor){
        final String rootPath = "/home/soslan/Desktop/data/";
        final String path = rootPath + sizeFactor + "/" + file + ".tbl";

        System.out.println("Import data from " + file + " csv");
        System.out.println("Start reading data");
        long start = System.currentTimeMillis();
        Dataset<Row> dataFrame = spark.read()
                .option("header", false)
                .option("delimiter","|")
                .format("csv")
                .schema(createSchema(file))
                .csv(path);
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
        System.out.println("Elapsed time to read: = " + elapsedTimeSeconds + " seconds");

        validateDimensions(spark, dataFrame,file);
    }
}
