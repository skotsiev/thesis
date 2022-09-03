package etl.delta;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static etl.common.Utils.elapsedTime;

public class LoadDelta {

    final static Logger logger = LogManager.getLogger(LoadDelta.class);

    public LoadDelta(String name, String sizeFactor) {
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final String name;
    private final String sizeFactor;

    public void overwriteToDelta(SparkSession spark, Dataset<Row> data) {
        long start = System.currentTimeMillis();
        long count = data.count();
        data.write()
                .format("delta")
                .mode("overwrite")
                .save("/tmp/delta-" + name + sizeFactor);

        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTime(elapsedTime);
        System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "Write " + count + " lines " + name + ": " + elapsedTimeString);
//        logger.info("[" + getClass().getSimpleName() + "]\t\t" + "Write\t" + count + "\tlines:" + elapsedTimeString);
    }

    public void overwriteToDelta(SparkSession spark, Dataset<Row> data, boolean withLogs) {
        data.write()
                .format("delta")
                .mode("overwrite")
                .save("/tmp/delta-" + name + sizeFactor);
    }

    public void appendToDelta(Dataset<Row> data, Boolean flag) {
        String path = "/tmp/delta-";
        long count = data.count();

        if (flag) {
            path += name + sizeFactor;
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
