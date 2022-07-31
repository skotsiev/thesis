package spark.common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Schemas {

    public static StructType createSchema(String schemaType) {

        switch(schemaType.toLowerCase()) {
            case "customer": {
                return new StructType()
                        .add("c_custkey", DataTypes.IntegerType)
                        .add("c_name", DataTypes.StringType)
                        .add("c_address", DataTypes.StringType)
                        .add("c_nationkey", DataTypes.IntegerType)
                        .add("c_phone", DataTypes.StringType)
                        .add("c_acctbal", DataTypes.DoubleType)
                        .add("c_mktsegment", DataTypes.StringType)
                        .add("c_comment", DataTypes.StringType);
            }
            case "lineitem": {
                return new StructType()
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
            }
            case "nation": {
                return new StructType()
                        .add("n_nationkey", DataTypes.IntegerType)
                        .add("n_name", DataTypes.StringType)
                        .add("n_regionkey", DataTypes.IntegerType)
                        .add("n_comment", DataTypes.StringType);
            }
            case "orders": {
                return new StructType()
                        .add("o_orderkey", DataTypes.IntegerType)
                        .add("o_custkey", DataTypes.IntegerType)
                        .add("o_orderstatus", DataTypes.StringType)
                        .add("o_totalprice", DataTypes.DoubleType)
                        .add("o_orderdate", DataTypes.StringType)
                        .add("o_orderpriority", DataTypes.StringType)
                        .add("o_clerk", DataTypes.IntegerType)
                        .add("o_shippriority", DataTypes.IntegerType)
                        .add("o_comment", DataTypes.StringType);
            }
            case "part": {
                return new StructType()
                        .add("p_partkey", DataTypes.IntegerType)
                        .add("p_name", DataTypes.StringType)
                        .add("p_mfgr", DataTypes.StringType)
                        .add("p_brand", DataTypes.StringType)
                        .add("p_type", DataTypes.StringType)
                        .add("p_size", DataTypes.IntegerType)
                        .add("p_container", DataTypes.StringType)
                        .add("p_retailprice", DataTypes.DoubleType)
                        .add("p_comment", DataTypes.StringType);
            }
            case "partsupp": {
                return new StructType()
                        .add("ps_partkey", DataTypes.IntegerType)
                        .add("ps_suppkey", DataTypes.IntegerType)
                        .add("ps_availqty", DataTypes.IntegerType)
                        .add("ps_supplycost", DataTypes.DoubleType)
                        .add("ps_comment", DataTypes.StringType);
            }
            case "region": {
                return new StructType()
                        .add("r_regionkey", DataTypes.IntegerType)
                        .add("r_name", DataTypes.StringType)
                        .add("r_comment", DataTypes.StringType);
            }
            case "supplier": {
                return new StructType()
                        .add("s_suppkey", DataTypes.IntegerType)
                        .add("s_name", DataTypes.StringType)
                        .add("s_address", DataTypes.StringType)
                        .add("s_nationkey", DataTypes.IntegerType)
                        .add("s_phone", DataTypes.StringType)
                        .add("s_acctbal", DataTypes.DoubleType)
                        .add("s_comment", DataTypes.StringType);
            }
        }
        return null;
    }
}
