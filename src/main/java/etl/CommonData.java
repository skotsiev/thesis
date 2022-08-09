package etl;

import java.util.ArrayList;
import java.util.HashMap;

public class CommonData {

    public class TableObj {

        ArrayList<String> primaryKeys = new ArrayList<String>();
        ArrayList<String> foreignKeys = new ArrayList<String>();

        public ArrayList<String> getPrimaryKeys() {
            return primaryKeys;
        }
        public ArrayList<String> getForeignKeys() {
            return foreignKeys;
        }

        public void addPrimaryKeys(String key) {
            this.primaryKeys.add(key);
        }
        public void addForeignKeys(String key) {
            this.foreignKeys.add(key);
        }
    }


    public TableObj tableInfo(String name) {

        HashMap<String, String> hashMap = new HashMap<>();
        TableObj tableObj = new TableObj();
        switch(name){
            case "customer": {
                tableObj.addPrimaryKeys("c_custkey");
                tableObj.addForeignKeys("c_nationkey");
                tableObj.addForeignKeys("n_nationkey");

                hashMap.put("pk#","1");
                hashMap.put("fk#","1");
                hashMap.put("pk1","c_custkey");
                hashMap.put("fk1","c_nationkey");
                hashMap.put("fk2","n_nationkey");
                break;
            }
            case "lineitem": {
                tableObj.addPrimaryKeys("l_orderkey");
                tableObj.addPrimaryKeys("l_linenumber");
                tableObj.addForeignKeys("l_partkey");
                tableObj.addForeignKeys("p_partkey");
                tableObj.addForeignKeys("l_suppkey");
                tableObj.addForeignKeys("s_suppkey");

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
                tableObj.addPrimaryKeys("n_nationkey");
                tableObj.addForeignKeys("n_regionkey");
                tableObj.addForeignKeys("r_regionkey");

                hashMap.put("pk#","1");
                hashMap.put("fk#","1");
                hashMap.put("pk1","n_nationkey");
                hashMap.put("fk1","n_regionkey");
                hashMap.put("fk2","r_regionkey");
                break;
            }
            case "orders": {
                tableObj.addPrimaryKeys("o_orderkey");
                tableObj.addForeignKeys("o_custkey");
                tableObj.addForeignKeys("c_custkey");

                hashMap.put("pk#","1");
                hashMap.put("fk#","1");
                hashMap.put("pk1","o_orderkey");
                hashMap.put("fk1","o_custkey");
                hashMap.put("fk2","c_custkey");
                break;
            }
            case "part": {
                tableObj.addPrimaryKeys("p_partkey");

                hashMap.put("pk#","1");
                hashMap.put("fk#","0");
                hashMap.put("pk1","p_partkey");
                break;
            }
            case "partsupp": {
                tableObj.addPrimaryKeys("ps_partkey");
                tableObj.addPrimaryKeys("ps_suppkey");
                tableObj.addForeignKeys("ps_partkey");
                tableObj.addForeignKeys("p_partkey");
                tableObj.addForeignKeys("ps_suppkey");
                tableObj.addForeignKeys("s_suppkey");

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
                tableObj.addPrimaryKeys("r_regionkey");

                hashMap.put("pk1","r_regionkey");
                break;
            }
            case "supplier": {
                tableObj.addPrimaryKeys("s_suppkey");
                tableObj.addForeignKeys("s_nationkey");
                tableObj.addForeignKeys("n_nationkey");

                hashMap.put("pk1","s_suppkey");
                hashMap.put("fk1","n_nationkey");
                break;
            }
        }
        return tableObj;
    }

}
