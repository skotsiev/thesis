package etl.flink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.concurrent.TimeUnit;

import static etl.common.Constants.MYSQL_URL;
import static etl.common.Constants.connectionProperties;
import static org.apache.spark.sql.functions.current_timestamp;

public class LoadFlink {

    public LoadFlink(String name) {
        this.name = name;
    }
    private final String name;

    public void overwriteToMysql(Dataset<Row> data, String schema){
        data.show();

        System.out.println("[" + getClass().getSimpleName() + "]" + "\t\t\toverwriteToMysql: " + schema+ "." + name );
        long start = System.currentTimeMillis();
        try{
            data
                    .withColumn("register_date", current_timestamp())
                    .write()
                    .option("drop", "true")
                    .mode("overwrite")
                    .jdbc(MYSQL_URL, schema+ "." + name, connectionProperties());
            long end = System.currentTimeMillis();
            long elapsedTime = end - start;
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
            System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "Elapsed time to write: = " + elapsedTimeSeconds + " sec");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void appendToMysql(Dataset<Row> data, Boolean flag){
        String table = "warehouse.";
        if (flag) {
            table += name;
        } else {
            table += name + "_rejected";
        }
        System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "appendToMysql: " + table);
        System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" +  "data count: "+ data.count() );
        long start = System.currentTimeMillis();
        data
                .withColumn("register_date", current_timestamp())
                .write()
                .mode("Append")
                .jdbc(MYSQL_URL, table, connectionProperties());

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
