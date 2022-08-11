package etl;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import spark.common.Initializer;

import java.util.ArrayList;

import static etl.Load.writeToMysql;
import static org.apache.spark.sql.functions.col;

public class Transform {

    public static void validatePrimaryKey(SparkSession spark, Dataset<Row> dataFrame, String name){
        CommonData commonData = new CommonData();
        int primaryKeyCount = commonData.tableInfo(name).getPrimaryKeys().size();

        Column[] primaryKeyColumn = new Column[primaryKeyCount];
        ArrayList<String> primaryKeys = commonData.tableInfo(name).getPrimaryKeys();

        for(int j = 0 ; j < primaryKeyCount; j++){
            primaryKeyColumn[j] = col(primaryKeys.get(j));
            System.out.println("primaryKeyColumn " + primaryKeyColumn[j]);
        }

        Dataset<Row> dataframeFromDB = spark.
                read()
                .jdbc("jdbc:mysql://localhost:3306", "warehouse." + name, Initializer.connectionProperties())
                .select(primaryKeyColumn);

        Dataset<Row> newDataFrameKeys = dataFrame
                .select(primaryKeyColumn);

        Dataset<Row> validData;
        Dataset<Row> invalidData;

        if (primaryKeyCount == 1){
            validData = newDataFrameKeys
                    .except(dataframeFromDB)
                    .join(dataFrame, dataFrame.col(primaryKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(0))))
                    .drop(dataFrame.col(primaryKeys.get(0)));

            invalidData = newDataFrameKeys
                    .intersect(dataframeFromDB)
                    .join(dataFrame, dataFrame.col(primaryKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(0))))
                    .drop(dataFrame.col(primaryKeys.get(0)));

        } else {
            validData = newDataFrameKeys
                    .except(dataframeFromDB)
                    .join(dataFrame, (dataFrame.col(primaryKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(0)))
                            .$amp$amp(dataFrame.col(primaryKeys.get(1)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(1))))))
                    .drop(dataFrame.col(primaryKeys.get(0)))
                    .drop(dataFrame.col(primaryKeys.get(1)));

            invalidData = newDataFrameKeys
                    .intersect(dataframeFromDB)
                    .join(dataFrame, (dataFrame.col(primaryKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(0)))
                            .$amp$amp(dataFrame.col(primaryKeys.get(1)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(1))))))
                    .drop(dataFrame.col(primaryKeys.get(0)))
                    .drop(dataFrame.col(primaryKeys.get(1)));
        }
        System.out.println("invalidData");
        if (invalidData != null) {
            invalidData.show();
        }
        System.out.println("validData");
        if (validData != null) {
            validData.show();
        }

    }

    public static void validateForeignKeys(SparkSession spark, Dataset<Row> dataFrame, String name){
        CommonData commonData = new CommonData();
        int foreignKeyCount = commonData.tableInfo(name).getPrimaryKeys().size();

        Column[] foreignKeyColumn = new Column[foreignKeyCount];
        ArrayList<String> foreignKeys = commonData.tableInfo(name).getForeignKeys();

        for(int j = 0 ; j < foreignKeyCount; j++){
            foreignKeyColumn[j] = col(foreignKeys.get(j));
            System.out.println("primaryKeyColumn " + foreignKeyColumn[j]);
        }

        Dataset<Row> dataframeFromDB = spark
                .read()
                .jdbc("jdbc:mysql://localhost:3306", "warehouse." + name, Initializer.connectionProperties())
                .select(foreignKeyColumn);

        Dataset<Row> newDataFrameKeys = dataFrame
                .select(foreignKeyColumn);

        Dataset<Row> validData;
        Dataset<Row> invalidData;

        if (foreignKeyCount == 1){
            validData = newDataFrameKeys
                    .except(dataframeFromDB)
                    .join(dataFrame, dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0))))
                    .drop(dataFrame.col(foreignKeys.get(0)));

            invalidData = newDataFrameKeys
                    .intersect(dataframeFromDB)
                    .join(dataFrame, dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0))))
                    .drop(dataFrame.col(foreignKeys.get(0)));

        } else {
            validData = newDataFrameKeys
                    .except(dataframeFromDB)
                    .join(dataFrame, (dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0)))
                            .$amp$amp(dataFrame.col(foreignKeys.get(1)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(1))))))
                    .drop(dataFrame.col(foreignKeys.get(0)))
                    .drop(dataFrame.col(foreignKeys.get(1)));

            invalidData = newDataFrameKeys
                    .intersect(dataframeFromDB)
                    .join(dataFrame, (dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0)))
                            .$amp$amp(dataFrame.col(foreignKeys.get(1)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(1))))))
                    .drop(dataFrame.col(foreignKeys.get(0)))
                    .drop(dataFrame.col(foreignKeys.get(1)));
        }
        System.out.println("invalidData");
        if (invalidData != null) {
            invalidData.show();
        }
        System.out.println("validData");
        if (validData != null) {
            validData.show();
            writeToMysql(validData, name);
        }
    }
}
