package etl;

import java.util.HashMap;

public class CommonData {


    public static HashMap<String, String> tableInfo(String name) {

        HashMap<String, String> hashMap = new HashMap<>();

        switch(name){
            case "customer": {
                hashMap.put("pk#","1");
                hashMap.put("fk#","1");
                hashMap.put("pk1","c_custkey");
                hashMap.put("fk1","c_nationkey");
                hashMap.put("fk2","n_nationkey");
                break;
            }
            case "lineitem": {
                hashMap.put("pk#","2");
                hashMap.put("fk#","2");
                hashMap.put("pk1","l_orderkey");
                hashMap.put("pk2","l_linenumber");
                hashMap.put("fk11","l_partkey");
                hashMap.put("fk12","p_partkey");
                hashMap.put("fk21","l_suppkey");
                hashMap.put("fk22","s_suppkey");
                break;
            }
            case "nation": {
                hashMap.put("pk#","1");
                hashMap.put("fk#","1");
                hashMap.put("pk1","n_nationkey");
                hashMap.put("fk1","n_regionkey");
                hashMap.put("fk2","r_regionkey");
                break;
            }
            case "orders": {
                hashMap.put("pk#","1");
                hashMap.put("fk#","1");
                hashMap.put("pk1","o_orderkey");
                hashMap.put("fk1","o_custkey");
                hashMap.put("fk2","c_custkey");
                break;
            }
            case "part": {
                hashMap.put("pk#","1");
                hashMap.put("fk#","0");
                hashMap.put("pk1","p_partkey");
                break;
            }
            case "partsupp": {
                hashMap.put("pk#","2");
                hashMap.put("fk#","2");
                hashMap.put("pk1","ps_partkey");
                hashMap.put("pk2","ps_suppkey");
                hashMap.put("fk11","ps_partkey");
                hashMap.put("fk12","p_partkey");
                hashMap.put("fk21","ps_suppkey");
                hashMap.put("fk22","s_suppkey");
                break;
            }
            case "region": {
                hashMap.put("pk1","r_regionkey");
                break;
            }
            case "supplier": {
                hashMap.put("pk1","s_suppkey");
                hashMap.put("fk1","n_nationkey");
                break;
            }
        }
        return hashMap;
    }
}
