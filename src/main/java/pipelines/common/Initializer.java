package pipelines.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Properties;

import static etl.common.Constants.*;

public class Initializer {
    static public Properties connectionProperties(){
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "root");
        return connectionProperties;
    }
    static public void initJdbc(SparkSession spark){
        System.out.print("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "Initialization start...");
        ArrayList<String> tableList = tableList();

        for(String i : tableList){
            String tableName = "warehouse." + i;
            Dataset<Row> dataFrame = spark.read().jdbc(MYSQL_URL, tableName, Initializer.connectionProperties());
            dataFrame.createOrReplaceTempView(i);
        }
        System.out.println("Done");
    }

    static public void initDelta(SparkSession spark){
        System.out.print("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "Initialization start...");
        ArrayList<String> tableList = tableList();

        for(String i : tableList){
            String tablePath = "/tmp/delta-" + i;
            Dataset<Row> deltaTable = spark.read().format("delta").load(tablePath);
            deltaTable.createOrReplaceTempView(i);
        }
        System.out.println("Done");
    }

}
