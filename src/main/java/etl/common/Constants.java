package etl.common;

import java.util.ArrayList;
import java.util.Properties;

public class Constants {
    public static final String ROOT_CSV_PATH = "/home/soslan/Desktop/data/";
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306";

    static public Properties connectionProperties() {
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "root");
        return connectionProperties;
    }

    static public ArrayList<String> tableList() {
        final ArrayList<String> tableList = new ArrayList<>();
        tableList.add("region");
        tableList.add("nation");
        tableList.add("supplier");
        tableList.add("customer");
        tableList.add("part");
        tableList.add("partsupp");
        tableList.add("orders");
        tableList.add("lineitem");
        return tableList;
    }

    static public ArrayList<String> queriesList() {
        final ArrayList<String> queriesList = new ArrayList<>();
        queriesList.add("q01");
        queriesList.add("q02");
        queriesList.add("q03");
        queriesList.add("q04");
        queriesList.add("q05");
        queriesList.add("q06");
        queriesList.add("q07");
        queriesList.add("q08");
        queriesList.add("q09");
        queriesList.add("q10");
        queriesList.add("q11");
        queriesList.add("q12");
        queriesList.add("q13");
        queriesList.add("q14");
        queriesList.add("q16");
        queriesList.add("q17");
        queriesList.add("q18");
        queriesList.add("q19");
        queriesList.add("q20");
        queriesList.add("q21");
        queriesList.add("q22");
        return queriesList;
    }

    static public ArrayList<String> sizeFactorList() {
        final ArrayList<String> sizeFactorList = new ArrayList<>();
        sizeFactorList.add("100MB");
        sizeFactorList.add("1GB");
        sizeFactorList.add("5GB");
        return sizeFactorList;
    }

    static public ArrayList<String> tableColumnList(String name) {
        final ArrayList<String> tableColumnList = new ArrayList<>();

        switch (name) {
            case "customer": {
                tableColumnList.add("c_custkey");
                tableColumnList.add("c_name");
                tableColumnList.add("c_address");
                tableColumnList.add("c_nationkey");
                tableColumnList.add("c_phone");
                tableColumnList.add("c_acctbal");
                tableColumnList.add("c_mktsegment");
                tableColumnList.add("c_comment");
                break;
            }
            case "lineitem": {
                tableColumnList.add("l_orderkey");
                tableColumnList.add("l_partkey");
                tableColumnList.add("l_suppkey");
                tableColumnList.add("l_linenumber");
                tableColumnList.add("l_quantity");
                tableColumnList.add("l_extendedprice");
                tableColumnList.add("l_discount");
                tableColumnList.add("l_tax");
                tableColumnList.add("l_returnflag");
                tableColumnList.add("l_linestatus");
                tableColumnList.add("l_shipdate");
                tableColumnList.add("l_commitdate");
                tableColumnList.add("l_receiptdate");
                tableColumnList.add("l_shipinstruct");
                tableColumnList.add("l_shipmode");
                tableColumnList.add("l_comment");
                break;
            }
            case "nation": {
                tableColumnList.add("n_nationkey");
                tableColumnList.add("n_name");
                tableColumnList.add("n_regionkey");
                tableColumnList.add("n_comment");
                break;
            }
            case "orders": {
                tableColumnList.add("o_orderkey");
                tableColumnList.add("o_custkey");
                tableColumnList.add("o_orderstatus");
                tableColumnList.add("o_totalprice");
                tableColumnList.add("o_orderdate");
                tableColumnList.add("o_orderpriority");
                tableColumnList.add("o_clerk");
                tableColumnList.add("o_shippriority");
                tableColumnList.add("o_comment");
                break;
            }
            case "part": {
                tableColumnList.add("p_partkey");
                tableColumnList.add("p_name");
                tableColumnList.add("p_mfgr");
                tableColumnList.add("p_brand");
                tableColumnList.add("p_type");
                tableColumnList.add("p_size");
                tableColumnList.add("p_container");
                tableColumnList.add("p_retailprice");
                tableColumnList.add("p_comment");
                break;
            }
            case "partsupp": {
                tableColumnList.add("ps_partkey");
                tableColumnList.add("ps_suppkey");
                tableColumnList.add("ps_availqty");
                tableColumnList.add("ps_supplycost");
                tableColumnList.add("ps_comment");
                break;
            }
            case "region": {
                tableColumnList.add("r_regionkey");
                tableColumnList.add("r_name");
                tableColumnList.add("r_comment");
                break;
            }
            case "supplier": {
                tableColumnList.add("s_suppkey");
                tableColumnList.add("s_name");
                tableColumnList.add("s_address");
                tableColumnList.add("s_nationkey");
                tableColumnList.add("s_phone");
                tableColumnList.add("s_acctbal");
                tableColumnList.add("s_comment");
                break;
            }
        }
        return tableColumnList;
    }
}
