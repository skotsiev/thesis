package etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import spark.common.Initializer;

import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.current_timestamp;

public class Load {

    static public void writeToMysql(Dataset<Row> dataFrame, String name){
        dataFrame.show();

        System.out.println("Start writing data to DB");
        long start = System.currentTimeMillis();
        dataFrame
                .withColumn("register_date", current_timestamp())
                .withColumn("update_date", current_timestamp())
                .write()
                .mode("Append")
//                .option("drop", "true")
//                .mode("overwrite")
                .jdbc("jdbc:mysql://localhost:3306", "warehouse." + name, Initializer.connectionProperties());
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
        System.out.println("Elapsed time to write: = " + elapsedTimeSeconds + " sec");
    }
}
