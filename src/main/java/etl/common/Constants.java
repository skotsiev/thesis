package etl.common;

import java.util.ArrayList;

public class Constants {
    public static final String ROOT_CSV_PATH = "/home/soslan/Desktop/data/";
    public static final String ROOT_DELTA_PATH = "/tmp/";
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306";

    static public ArrayList<String> tableList() {
        final ArrayList<String> tableList = new ArrayList<>();
        tableList.add("customer");
        tableList.add("lineitem");
        tableList.add("nation");
        tableList.add("orders");
        tableList.add("part");
        tableList.add("partsupp");
        tableList.add("region");
        tableList.add("supplier");
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

}
