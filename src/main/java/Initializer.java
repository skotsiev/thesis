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

    static public void createViews(SparkSession spark){

        Dataset<Row> CustomerTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.CUSTOMER", Initializer.connectionProperties());
        Dataset<Row> LineItemTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.LINEITEM", Initializer.connectionProperties());
        Dataset<Row> NationTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.NATION", Initializer.connectionProperties());
        Dataset<Row> OrdersTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.ORDERS", Initializer.connectionProperties());
        Dataset<Row> PartTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.PART", Initializer.connectionProperties());
        Dataset<Row> PartSuppTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.PARTSUPP", Initializer.connectionProperties());
        Dataset<Row> RegionTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.REGION", Initializer.connectionProperties());
        Dataset<Row> SupplierTable = spark.read().jdbc("jdbc:mysql://localhost:3306", "tpch.SUPPLIER", Initializer.connectionProperties());

        CustomerTable.createOrReplaceTempView("CUSTOMER");
        LineItemTable.createOrReplaceTempView("LINEITEM");
        NationTable.createOrReplaceTempView("NATION");
        OrdersTable.createOrReplaceTempView("ORDERS");
        PartTable.createOrReplaceTempView("PART");
        PartSuppTable.createOrReplaceTempView("PARTSUPP");
        RegionTable.createOrReplaceTempView("REGION");
        SupplierTable.createOrReplaceTempView("SUPPLIER");
    }
}
