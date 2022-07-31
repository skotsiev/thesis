package etl;

import java.util.HashMap;

public class CommonData {

    public static HashMap<String, String> customer = new HashMap<>();
    public static HashMap<String, String> nation = new HashMap<>();
    public static HashMap<String, String> part = new HashMap<>();
    public static HashMap<String, String> partsupp = new HashMap<>();
    public static HashMap<String, String> region = new HashMap<>();
    public static HashMap<String, String> orders = new HashMap<>();

    static{
        //queries for batch execution


        region.put("pk","r_regionkey");

        part.put("pk","p_partkey");

        nation.put("pk","n_nationkey");
        nation.put("fk","regionkey");

        customer.put("pk","c_custkey");
        customer.put("fk","nationkey");

        orders.put("pk","o_orderkey");
        orders.put("fk","custkey");

        partsupp.put("pk","c_custkey");


    }
}
