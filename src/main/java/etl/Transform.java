package etl;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import spark.common.Initializer;

import static etl.CommonData.tableInfo;
import static etl.Load.writeToMysql;
import static org.apache.spark.sql.functions.col;

public class Transform {

    public static void validateDimensions(SparkSession spark, Dataset<Row> dataFrame, String name){
        int primaryKeyCount = Integer.valueOf(tableInfo(name).get("pk#"));
        Column[] primaryKeyColumn = new Column[primaryKeyCount];
        String primaryKey = tableInfo(name).get("pk1");

        for (int i = 0; i < primaryKeyCount; i++){
            String columnName = "pk" + String.valueOf(i + 1) ;
            System.out.println("columnName " + columnName);
            Column column = col(tableInfo(name).get(columnName));
            System.out.println("column " + column);

            primaryKeyColumn[i] =  column;
        }

        Dataset<Row> dataframeFromDB = spark.
                read()
                .jdbc("jdbc:mysql://localhost:3306", "warehouse." + name, Initializer.connectionProperties())
                .select(primaryKeyColumn);

        Dataset<Row> newDataFrameKeys = dataFrame
                .select(primaryKeyColumn);

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
