package spark.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static spark.common.Schemas.createSchema;

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
                .schema(createSchema("lineitem"))
                .csv(lineitemFile);

        Dataset<Row> ordersStream = spark.readStream()
                .option("header", false)
                .option("delimiter","|")
                .format("csv")

                .schema(createSchema("orders"))
                .csv(ordersFile);

        Dataset<Row> customerTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "warehouse.customer", Initializer.connectionProperties());
        Dataset<Row> lineItemTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "warehouse.lineitem", Initializer.connectionProperties());
        Dataset<Row> nationTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "warehouse.nation", Initializer.connectionProperties());
        Dataset<Row> ordersTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "warehouse.orders", Initializer.connectionProperties());
        Dataset<Row> partTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "warehouse.part", Initializer.connectionProperties());
        Dataset<Row> partSuppTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "warehouse.partsupp", Initializer.connectionProperties());
        Dataset<Row> regionTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "warehouse.region", Initializer.connectionProperties());
        Dataset<Row> supplierTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "warehouse.supplier", Initializer.connectionProperties());

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
