package etl.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static etl.common.Constants.ROOT_CSV_PATH;
import static etl.common.Schemas.createSchema;
import static etl.common.Utils.elapsedTimeSeconds;

public class ExtractSpark {

    public ExtractSpark(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name;
    private final String sizeFactor;


    public Dataset<Row> extractFromCsv(Boolean updateFlag) {

        final String path;
        if (updateFlag) path = ROOT_CSV_PATH + "/updates/*" + name + "*";
        else path = ROOT_CSV_PATH + sizeFactor + "/original/" + name + ".tbl";

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
        String elapsedTimeString = elapsedTimeSeconds(elapsedTime);
        System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Read " + dataFrame.count() + " lines from " + name + ": " + elapsedTimeString);
//        logger.info("[" + getClass().getSimpleName() + "]\t" + "Read\t" + dataFrame.count() + "\tlines:" + elapsedTimeString);
        return dataFrame;
    }

    public Dataset<Row> multipleUpdate(int index) {
        final String  path = ROOT_CSV_PATH + "/updates/*" + name + ".tbl.u*" + index;
        return spark.read()
                .option("header", false)
                .option("delimiter", "|")
                .format("csv")
                .schema(createSchema(name))
                .csv(path);
    }
}
