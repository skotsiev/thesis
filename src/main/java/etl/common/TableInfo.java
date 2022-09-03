package etl.common;

import java.util.ArrayList;

public class TableInfo {

    public TableInfo(String name) {
        switch (name) {
            case "customer": {
                this.addPrimaryKeys("c_custkey");
                this.addForeignKeys("c_nationkey");
                this.addForeignKeys("n_nationkey");
                this.addForeignKeyTable("nation");
                break;
            }
            case "lineitem": {
                this.addPrimaryKeys("l_orderkey");
                this.addPrimaryKeys("l_linenumber");
                this.addForeignKeys("l_partkey");
                this.addForeignKeys("p_partkey");
                this.addForeignKeys("l_suppkey");
                this.addForeignKeys("s_suppkey");
                this.addForeignKeyTable("part");
                this.addForeignKeyTable("supplier");
                break;
            }
            case "nation": {
                this.addPrimaryKeys("n_nationkey");
                this.addForeignKeys("n_regionkey");
                this.addForeignKeys("r_regionkey");
                this.addForeignKeyTable("region");
                break;
            }
            case "orders": {
                this.addPrimaryKeys("o_orderkey");
                this.addForeignKeys("o_custkey");
                this.addForeignKeys("c_custkey");
                this.addForeignKeyTable("customer");
                break;
            }
            case "part": {
                this.addPrimaryKeys("p_partkey");
                break;
            }
            case "partsupp": {
                this.addPrimaryKeys("ps_partkey");
                this.addPrimaryKeys("ps_suppkey");
                this.addForeignKeys("ps_partkey");
                this.addForeignKeys("p_partkey");
                this.addForeignKeys("ps_suppkey");
                this.addForeignKeys("s_suppkey");
                this.addForeignKeyTable("part");
                this.addForeignKeyTable("supplier");
                break;
            }
            case "region": {
                this.addPrimaryKeys("r_regionkey");
                break;
            }
            case "supplier": {
                this.addPrimaryKeys("s_suppkey");
                this.addForeignKeys("s_nationkey");
                this.addForeignKeys("n_nationkey");
                this.addForeignKeyTable("nation");
                break;
            }
        }
    }

    private final ArrayList<String>  primaryKeys = new ArrayList<>();
    private final ArrayList<String> foreignKeys = new ArrayList<>();
    private final ArrayList<String> foreignKeyTable = new ArrayList<>();

    public ArrayList<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public ArrayList<String> getForeignKeys() {
        return foreignKeys;
    }

    public ArrayList<String> getForeignKeyTable() {
        return foreignKeyTable;
    }

    private void addPrimaryKeys(String key) {
        this.primaryKeys.add(key);
    }

    private void addForeignKeys(String key) {
        this.foreignKeys.add(key);
    }

    private void addForeignKeyTable(String key) {
        this.foreignKeyTable.add(key);
    }

}
