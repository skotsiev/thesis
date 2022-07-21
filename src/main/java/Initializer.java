import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

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
    static public void createScema(SparkSession spark){

        StructType schemaCustomer = new StructType()
                .add("c_custkey", DataTypes.IntegerType)
                .add("c_name", DataTypes.StringType)
                .add("c_address", DataTypes.StringType)
                .add("c_nationkey", DataTypes.IntegerType)
                .add("c_phone", DataTypes.StringType)
                .add("c_acctbal", DataTypes.DoubleType)
                .add("c_mktsegment", DataTypes.StringType)
                .add("c_comment", DataTypes.StringType);

        StructType schemaLineitem = new StructType()
                .add("l_orderkey", DataTypes.IntegerType)
                .add("l_partkey", DataTypes.IntegerType)
                .add("l_suppkey", DataTypes.IntegerType)
                .add("l_linenumber", DataTypes.IntegerType)
                .add("l_quantity", DataTypes.IntegerType)
                .add("l_extendedprice", DataTypes.DoubleType)
                .add("l_discount", DataTypes.DoubleType)
                .add("l_tax", DataTypes.DoubleType)
                .add("l_returnflag", DataTypes.StringType)
                .add("l_linestatus", DataTypes.StringType)
                .add("l_shipdate", DataTypes.StringType)
                .add("l_commitdate", DataTypes.StringType)
                .add("l_receiptdate", DataTypes.StringType)
                .add("l_shipinstruct", DataTypes.StringType)
                .add("l_shipmode", DataTypes.StringType)
                .add("l_comment", DataTypes.StringType);

        StructType schemaNation = new StructType()
                .add("n_nationkey", DataTypes.IntegerType)
                .add("n_name", DataTypes.StringType)
                .add("n_regionkey", DataTypes.IntegerType)
                .add("n_comment", DataTypes.StringType);

        StructType schemaOrders = new StructType()
                .add("o_orderkey", DataTypes.IntegerType)
                .add("o_custkey", DataTypes.IntegerType)
                .add("o_orderstatus", DataTypes.StringType)
                .add("o_totalprice", DataTypes.DoubleType)
                .add("o_orderdate", DataTypes.StringType)
                .add("o_orderpriority", DataTypes.StringType)
                .add("o_clerk", DataTypes.IntegerType)
                .add("o_shippriority", DataTypes.IntegerType)
                .add("o_comment", DataTypes.StringType);

        StructType schemaPart = new StructType()
                .add("p_partkey", DataTypes.IntegerType)
                .add("p_name", DataTypes.StringType)
                .add("p_mfgr", DataTypes.StringType)
                .add("p_brand", DataTypes.StringType)
                .add("p_type", DataTypes.StringType)
                .add("p_size", DataTypes.IntegerType)
                .add("p_container", DataTypes.StringType)
                .add("p_retailprice", DataTypes.DoubleType)
                .add("p_comment", DataTypes.StringType);

        StructType schemaPartsupp = new StructType()
                .add("ps_partkey", DataTypes.IntegerType)
                .add("ps_suppkey", DataTypes.IntegerType)
                .add("ps_availqty", DataTypes.IntegerType)
                .add("ps_supplycost", DataTypes.DoubleType)
                .add("ps_comment", DataTypes.StringType);

        StructType schemaRegion = new StructType()
                .add("r_regionkey", DataTypes.IntegerType)
                .add("r_name", DataTypes.StringType)
                .add("r_comment", DataTypes.StringType);

        StructType schemaSupplier = new StructType()
                .add("s_suppkey", DataTypes.IntegerType)
                .add("s_name", DataTypes.StringType)
                .add("s_address", DataTypes.StringType)
                .add("s_nationkey", DataTypes.IntegerType)
                .add("s_phone", DataTypes.StringType)
                .add("s_acctbal", DataTypes.DoubleType)
                .add("s_comment", DataTypes.StringType);

        final String customerFile = "/home/soslan/Desktop/data/0.1GB/customer.tbl";
        final String lineitemFile = "/home/soslan/Desktop/data/0.1GB/lineitem.tbl";
        final String nationFile = "/home/soslan/Desktop/data/0.1GB/nation.tbl";
        final String ordersFile = "/home/soslan/Desktop/data/0.1GB/orders.tbl";
        final String partFile = "/home/soslan/Desktop/data/0.1GB/part.tbl";
        final String partsuppFile = "/home/soslan/Desktop/data/0.1GB/partsupp.tbl";
        final String regionFile = "/home/soslan/Desktop/data/0.1GB/region.tbl";
        final String supplierFile = "/home/soslan/Desktop/data/0.1GB/supplier.tbl";






    }

}
