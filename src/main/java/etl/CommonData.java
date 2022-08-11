package etl;

import java.util.ArrayList;

public class CommonData {

    public class TableObj {

        ArrayList<String> primaryKeys = new ArrayList<>();
        ArrayList<String> foreignKeys = new ArrayList<>();

        ArrayList<String> foreignKeyTable = new ArrayList<>();


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

        public void addForeignKeyTable(String key) {
            this.foreignKeyTable.add(key);
        }
    }


    public TableObj tableInfo(String name) {

        TableObj tableObj = new TableObj();
        switch(name){
            case "customer": {
                tableObj.addPrimaryKeys("c_custkey");
                tableObj.addForeignKeys("c_nationkey");
                tableObj.addForeignKeys("n_nationkey");
                tableObj.addForeignKeyTable("nation");
                break;
            }
            case "lineitem": {
                tableObj.addPrimaryKeys("l_orderkey");
                tableObj.addPrimaryKeys("l_linenumber");
                tableObj.addForeignKeys("l_partkey");
                tableObj.addForeignKeys("p_partkey");
                tableObj.addForeignKeys("l_suppkey");
                tableObj.addForeignKeys("s_suppkey");
                tableObj.addForeignKeyTable("part");
                tableObj.addForeignKeyTable("supplier");
                break;
            }
            case "nation": {
                tableObj.addPrimaryKeys("n_nationkey");
                tableObj.addForeignKeys("n_regionkey");
                tableObj.addForeignKeys("r_regionkey");
                tableObj.addForeignKeyTable("region");
                break;
            }
            case "orders": {
                tableObj.addPrimaryKeys("o_orderkey");
                tableObj.addForeignKeys("o_custkey");
                tableObj.addForeignKeys("c_custkey");
                tableObj.addForeignKeyTable("customer");
                break;
            }
            case "part": {
                tableObj.addPrimaryKeys("p_partkey");
                break;
            }
            case "partsupp": {
                tableObj.addPrimaryKeys("ps_partkey");
                tableObj.addPrimaryKeys("ps_suppkey");
                tableObj.addForeignKeys("ps_partkey");
                tableObj.addForeignKeys("p_partkey");
                tableObj.addForeignKeys("ps_suppkey");
                tableObj.addForeignKeys("s_suppkey");
                tableObj.addForeignKeyTable("partsupp");
                tableObj.addForeignKeyTable("supplier");
                break;
            }
            case "region": {
                tableObj.addPrimaryKeys("r_regionkey");
                break;
            }
            case "supplier": {
                tableObj.addPrimaryKeys("s_suppkey");
                tableObj.addForeignKeys("s_nationkey");
                tableObj.addForeignKeys("n_nationkey");
                tableObj.addForeignKeyTable("nation");
                break;
            }
        }
        return tableObj;
    }

}
