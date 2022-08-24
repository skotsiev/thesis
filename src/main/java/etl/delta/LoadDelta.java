package etl.delta;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static etl.common.Utils.elapsedTime;

public class LoadDelta {

    final static Logger logger = LogManager.getLogger(LoadDelta.class);

    public LoadDelta(String name) {
        this.name = name;
    }

    private final String name;

    public void overwriteToDelta(SparkSession spark, Dataset<Row> data) {
        long start = System.currentTimeMillis();
        long count = data.count();
        data.write()
                .format("delta")
                .mode("overwrite")
                .save("/tmp/delta-" + name);

        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTime(elapsedTime);

        logger.info("[" + getClass().getSimpleName() + "]\t\t" + "Elapsed time to write " + count + " lines to " + name + ": " + elapsedTimeString);
        Dataset<Row> df = spark.read().format("delta")
                .load("/tmp/delta-" + name)
                ;
        df.show();

    }

    public void appendToDelta(Dataset<Row> data, Boolean flag) {
        String path = "/tmp/delta-";
        long count = data.count();

        if (flag) {
            path += name;
        } else {
            path += name + "-rejected";
        }
        long start = System.currentTimeMillis();

        data.write()
                .format("delta")
                .mode("Append")
                .save(path);

        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTime(elapsedTime);
        logger.info("[" + getClass().getSimpleName() + "]\t\t" + "Elapsed time to write " + count + " lines to " + name + ": " + elapsedTimeString);
    }
}
