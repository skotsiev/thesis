package etl.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import spark.common.Initializer;

import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.current_timestamp;

public class Load {

    public Load(String name) {
        this.name = name;
    }
    static String name;

    public void overwriteToMysql(Dataset<Row> data){
        data.show();

        System.out.println("Start writing data to DB");
        long start = System.currentTimeMillis();
        try{
            data
                    .withColumn("register_date", current_timestamp())
                    .write()
                    .option("drop", "true")
                    .mode("overwrite")
                    .jdbc("jdbc:mysql://localhost:3306", "warehouse." + name, Initializer.connectionProperties());
            long end = System.currentTimeMillis();
            long elapsedTime = end - start;
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
            System.out.println("Elapsed time to write: = " + elapsedTimeSeconds + " sec");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void appendToMysql(Dataset<Row> data, Boolean flag){
        data.show();
        String table = "warehouse.";
        if (flag) {
            table += name;
        } else {
            table += name + "_rejected";
        }

        System.out.println("Start writing data to DB");
        long start = System.currentTimeMillis();
        data
                .withColumn("register_date", current_timestamp())
                .write()
                .mode("Append")
                .jdbc("jdbc:mysql://localhost:3306", table, Initializer.connectionProperties());

        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        if (elapsedTime < 1000){
            System.out.println("Elapsed time to write: " + elapsedTime + " millis");
        }
        else {
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
            System.out.println("Elapsed time to write: " + elapsedTimeSeconds + " seconds");
        }
    }

}
