package etl;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.collection.Seq;
import spark.common.Initializer;

import java.util.ArrayList;

import static etl.Load.writeToMysql;
import static org.apache.spark.sql.functions.col;

public class Transform {

    public static void validateDimensions(SparkSession spark, Dataset<Row> dataFrame, String name){
        CommonData commonData = new CommonData();
        int primaryKeyCount = commonData.tableInfo(name).getPrimaryKeys().size();
        int foreignKeyCount = commonData.tableInfo(name).getForeignKeys().size()/2;

        ArrayList<Column> primaryKeyColumn = new ArrayList<>();
        ArrayList<String> primaryKeys = commonData.tableInfo(name).getPrimaryKeys();

        for(String i : primaryKeys){
            primaryKeyColumn.add(col(i));
        }


//    for (int i = 0; i < primaryKeyCount; i++){
//
//
//            System.out.println("columnName " + columnName);
//            Column column = col(commonData.tableInfo(name).get(columnName));
//            System.out.println("column " + column);
//
//            primaryKeyColumn[i] =  col(primaryKey.get(i));
//
//        }

        Dataset<Row> dataframeFromDB = spark.
                read()
                .jdbc("jdbc:mysql://localhost:3306", "warehouse." + name, Initializer.connectionProperties())
                .select((Seq<Column>) primaryKeyColumn);

        Dataset<Row> newDataFrameKeys = dataFrame
                .select((Seq<Column>) primaryKeyColumn);

        Dataset<Row> validData = newDataFrameKeys
                .except(dataframeFromDB)
                .join(dataFrame, dataFrame.col(primaryKey).$eq$eq$eq(newDataFrameKeys.col(primaryKey)))
                .drop(dataFrame.col(primaryKey));

        Dataset<Row> invalidData = newDataFrameKeys
                .intersect(dataframeFromDB)
                .join(dataFrame, dataFrame.col(primaryKey).$eq$eq$eq(newDataFrameKeys.col(primaryKey)))
                .drop(dataFrame.col(primaryKey));

        dataframeFromDB.show();
        dataFrame.show();

        if (invalidData != null) invalidData.show();
        if (validData != null) {
            validData.show();
            writeToMysql(validData, name);
        }
    }
}
