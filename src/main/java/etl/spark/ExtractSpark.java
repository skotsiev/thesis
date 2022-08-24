package etl.spark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static etl.common.Constants.ROOT_CSV_PATH;
import static etl.common.Schemas.createSchema;
import static etl.common.Utils.elapsedTime;

public class ExtractSpark {

    final static Logger logger = LogManager.getLogger(ExtractSpark.class);

    public ExtractSpark(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name;
    private final String sizeFactor;


    public Dataset<Row> extractFromCsv(Boolean updateFlag) {
        final String rootPath = ROOT_CSV_PATH;
        final String path;
        if (updateFlag) path = rootPath + sizeFactor + "/update/" + name + ".tbl";
        else path = rootPath + sizeFactor + "/original/" + name + ".tbl";

        System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Import data from " + name + ".csv");
        System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Start reading data");
        long start = System.currentTimeMillis();
        Dataset<Row> dataFrame = spark.read()
                .option("header", false)
                .option("delimiter", "|")
                .format("csv")
                .schema(createSchema(name))
                .csv(path);
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTime(elapsedTime);
        System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Elapsed time to read " + dataFrame.count() + " lines from " + name + ": " + elapsedTimeString);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Elapsed time to read " + dataFrame.count() + " lines from " + name + ": " + elapsedTimeString);

        return dataFrame;
    }
}
