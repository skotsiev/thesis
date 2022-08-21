package pipelines.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static etl.common.Constants.MYSQL_URL;
import static pipelines.common.Schemas.createSchema;

public class Initializer {
    static public Properties connectionProperties(){
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "root");
        return connectionProperties;
    }
    static public void initJdbc(SparkSession spark){
        System.out.print("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "Initialization start...");
        final String lineitemFile = "/home/soslan/Desktop/data/0.1GB/lineitem*.tbl";
        final String ordersFile = "/home/soslan/Desktop/data/0.1GB/orders*.tbl";

        Dataset<Row> lineItemStream = spark.readStream()
                .option("header", false)
                .option("delimiter","|")
                .format("csv")
                .schema(createSchema("lineitem"))
                .csv(lineitemFile);

        Dataset<Row> ordersStream = spark.readStream()
                .option("header", false)
                .option("delimiter","|")
                .format("csv")

                .schema(createSchema("orders"))
                .csv(ordersFile);

        Dataset<Row> customerTable = spark.read().jdbc(MYSQL_URL, "warehouse.customer", Initializer.connectionProperties());
        Dataset<Row> lineItemTable = spark.read().jdbc(MYSQL_URL, "warehouse.lineitem", Initializer.connectionProperties());
        Dataset<Row> nationTable = spark.read().jdbc(MYSQL_URL, "warehouse.nation", Initializer.connectionProperties());
        Dataset<Row> ordersTable = spark.read().jdbc(MYSQL_URL, "warehouse.orders", Initializer.connectionProperties());
        Dataset<Row> partTable = spark.read().jdbc(MYSQL_URL, "warehouse.part", Initializer.connectionProperties());
        Dataset<Row> partSuppTable = spark.read().jdbc(MYSQL_URL, "warehouse.partsupp", Initializer.connectionProperties());
        Dataset<Row> regionTable = spark.read().jdbc(MYSQL_URL, "warehouse.region", Initializer.connectionProperties());
        Dataset<Row> supplierTable = spark.read().jdbc(MYSQL_URL, "warehouse.supplier", Initializer.connectionProperties());

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
        System.out.println("Done");
    }

    static public void initDelta(SparkSession spark){
        System.out.print("[" + Initializer.class.getSimpleName() + "]\t\t\t" + "Initialization start...");
        Dataset<Row> customerTable = spark.read().format("delta").load("/tmp/delta-customer");
        Dataset<Row> lineItemTable = spark.read().format("delta").load("/tmp/delta-lineitem");
        Dataset<Row> nationTable = spark.read().format("delta").load("/tmp/delta-nation");
        Dataset<Row> ordersTable = spark.read().format("delta").load("/tmp/delta-orders");
        Dataset<Row> partTable = spark.read().format("delta").load("/tmp/delta-part");
        Dataset<Row> partSuppTable = spark.read().format("delta").load("/tmp/delta-partsupp");
        Dataset<Row> regionTable = spark.read().format("delta").load("/tmp/delta-region");
        Dataset<Row> supplierTable = spark.read().format("delta").load("/tmp/delta-supplier");

        customerTable.createOrReplaceTempView("CUSTOMER");
        lineItemTable.createOrReplaceTempView("LINEITEM");
        nationTable.createOrReplaceTempView("NATION");
        ordersTable.createOrReplaceTempView("ORDERS");
        partTable.createOrReplaceTempView("PART");
        partSuppTable.createOrReplaceTempView("PARTSUPP");
        regionTable.createOrReplaceTempView("REGION");
        supplierTable.createOrReplaceTempView("SUPPLIER");
        System.out.println("Done");
    }

}
