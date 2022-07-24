package spark.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class Initializer {
    static public Properties connectionProperties(){
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "root");
        return connectionProperties;
    }
    static public void init(SparkSession spark){
        final String lineitemFile = "/home/soslan/Desktop/data/0.1GB/lineitem*.tbl";
        final String ordersFile = "/home/soslan/Desktop/data/0.1GB/orders*.tbl";

        Dataset<Row> lineItemStream = spark.readStream()
                .option("header", false)
                .option("delimiter","|")
                .format("csv")
                .schema(Schemas.schemaLineitem)
                .csv(lineitemFile);

        Dataset<Row> ordersStream = spark.readStream()
                .option("header", false)
                .option("delimiter","|")
                .format("csv")
                .schema(Schemas.schemaOrders)
                .csv(ordersFile);

        Dataset<Row> customerTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.CUSTOMER", Initializer.connectionProperties());
        Dataset<Row> lineItemTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.LINEITEM", Initializer.connectionProperties());
        Dataset<Row> nationTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.NATION", Initializer.connectionProperties());
        Dataset<Row> ordersTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.ORDERS", Initializer.connectionProperties());
        Dataset<Row> partTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.PART", Initializer.connectionProperties());
        Dataset<Row> partSuppTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.PARTSUPP", Initializer.connectionProperties());
        Dataset<Row> regionTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.REGION", Initializer.connectionProperties());
        Dataset<Row> supplierTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.SUPPLIER", Initializer.connectionProperties());

        customerTable.createOrReplaceTempView("CUSTOMER");
        lineItemTable.createOrReplaceTempView("LINEITEM");
        nationTable.createOrReplaceTempView("NATION");
        ordersTable.createOrReplaceTempView("ORDERS");
        partTable.createOrReplaceTempView("PART");
        partSuppTable.createOrReplaceTempView("PARTSUPP");
        regionTable.createOrReplaceTempView("REGION");
        supplierTable.createOrReplaceTempView("SUPPLIER");

        lineItemStream.createOrReplaceTempView("S_LINEITEM");
        ordersStream.createOrReplaceTempView("S_ORDERS");


    }
}
