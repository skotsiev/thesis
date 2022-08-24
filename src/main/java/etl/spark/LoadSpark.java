package etl.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static etl.common.Constants.MYSQL_URL;
import static etl.common.Constants.connectionProperties;
import static etl.common.Utils.elapsedTime;
import static org.apache.spark.sql.functions.current_timestamp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LoadSpark {
    final static Logger logger = LogManager.getLogger(LoadSpark.class);
    public LoadSpark(String name) {
        this.name = name;
    }
    String name;

    public void overwriteToMysql(Dataset<Row> data, String schema){
        long count = data.count();
        System.out.println("[" + getClass().getSimpleName() + "]" + "\t\t\toverwriteToMysql: " + schema+ "." + name );
        long start = System.currentTimeMillis();

        data
                .withColumn("register_date", current_timestamp())
                .write()
                .option("drop", "true")
                .mode("overwrite")
                .jdbc(MYSQL_URL, schema+ "." + name, connectionProperties());

        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTime(elapsedTime);

        logger.info("[" + getClass().getSimpleName() + "]\t\t" + "Elapsed time to write " + count + " lines to " + name + ": " + elapsedTimeString);
        System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "Elapsed time to write " + count + " lines to " + name + ": " + elapsedTimeString);
    }

    public void appendToMysql(Dataset<Row> data, Boolean flag){
        String table = "warehouse.";
        long count = data.count();

        if (flag) {
            table += name;
        } else {
            table += name + "_rejected";
        }

        System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "appendToMysql: " + table);
        System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" +  "data count: "+ count );
        long start = System.currentTimeMillis();
        data
                .withColumn("register_date", current_timestamp())
                .write()
                .mode("Append")
                .jdbc(MYSQL_URL, table, connectionProperties());

        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTime(elapsedTime);

        System.out.println("[" + getClass().getSimpleName() + "]\t\t\t" + "Elapsed time to update " + count + " lines to " + table + ": " + elapsedTimeString);
        logger.info("[" + getClass().getSimpleName() + "]\t\t" + "Elapsed time to update " + count + " lines to " + table + ": " + elapsedTimeString);
    }
}
