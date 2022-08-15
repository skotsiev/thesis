package etl.functions;

import etl.CommonData;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import spark.common.Initializer;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class Transform {

    public Transform(SparkSession spark, String name) {
        this.spark = spark;
        this.name = name;
    }

    private final SparkSession spark;
    private final String name;

    public Dataset<Row>  validDataPrimaryKeyCheck(Dataset<Row> dataFrame){
        CommonData commonData = new CommonData();
        int primaryKeyCount = commonData.tableInfo(name).getPrimaryKeys().size();

        Column[] primaryKeyColumn = new Column[primaryKeyCount];
        ArrayList<String> primaryKeys = commonData.tableInfo(name).getPrimaryKeys();

        for(int j = 0 ; j < primaryKeyCount; j++){
            primaryKeyColumn[j] = col(primaryKeys.get(j));
        }

        Dataset<Row> dataframeFromDB = spark.
                read()
                .jdbc("jdbc:mysql://localhost:3306", "warehouse." + name, Initializer.connectionProperties())
                .select(primaryKeyColumn);

        Dataset<Row> newDataFrameKeys = dataFrame
                .select(primaryKeyColumn);

        Dataset<Row> validData;

        if (primaryKeyCount == 1){
            validData = newDataFrameKeys
                    .except(dataframeFromDB)
                    .join(dataFrame, dataFrame.col(primaryKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(0))))
                    .drop(dataFrame.col(primaryKeys.get(0)));
        } else {
            validData = newDataFrameKeys
                    .except(dataframeFromDB)
                    .join(dataFrame, (dataFrame.col(primaryKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(0)))
                            .$amp$amp(dataFrame.col(primaryKeys.get(1)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(1))))))
                    .drop(dataFrame.col(primaryKeys.get(0)))
                    .drop(dataFrame.col(primaryKeys.get(1)));
        }
        System.out.println("validDataPrimaryKeyCheck done");
        validData.show();
        return validData;
    }

    public Dataset<Row>  invalidDataPrimaryKeyCheck(Dataset<Row> dataFrame){
        CommonData commonData = new CommonData();
        int primaryKeyCount = commonData.tableInfo(name).getPrimaryKeys().size();

        Column[] primaryKeyColumn = new Column[primaryKeyCount];
        ArrayList<String> primaryKeys = commonData.tableInfo(name).getPrimaryKeys();

        for(int j = 0 ; j < primaryKeyCount; j++){
            primaryKeyColumn[j] = col(primaryKeys.get(j));
        }

        Dataset<Row> dataframeFromDB = spark.
                read()
                .jdbc("jdbc:mysql://localhost:3306", "warehouse." + name, Initializer.connectionProperties())
                .select(primaryKeyColumn);

        Dataset<Row> newDataFrameKeys = dataFrame
                .select(primaryKeyColumn);

        Dataset<Row> invalidData;

        if (primaryKeyCount == 1){
            invalidData = newDataFrameKeys
                    .intersect(dataframeFromDB)
                    .join(dataFrame, dataFrame.col(primaryKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(0))))
                    .drop(dataFrame.col(primaryKeys.get(0)))
                    .withColumn("reject_reason",lit("foreign key violation"));

        } else {
            invalidData = newDataFrameKeys
                    .intersect(dataframeFromDB)
                    .join(dataFrame, (dataFrame.col(primaryKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(0)))
                            .$amp$amp(dataFrame.col(primaryKeys.get(1)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(1))))))
                    .drop(dataFrame.col(primaryKeys.get(0)))
                    .drop(dataFrame.col(primaryKeys.get(1)))
                    .withColumn("reject_reason",lit("primary key violation"));
        }
        System.out.println("invalidDataPrimaryKeyCheck done");
        invalidData.show();
        return invalidData;
    }

    public Dataset<Row> validDataForeignKeyCheck(Dataset<Row> dataFrame) {
        CommonData commonData = new CommonData();
        int foreignKeyCount = commonData.tableInfo(name).getForeignKeys().size();

        Dataset<Row> validData;
        System.out.println("foreignKeyCount " + commonData.tableInfo(name).getPrimaryKeys());
        if (foreignKeyCount >= 1){
            System.out.println("inside validDataForeignKeyCheck loop");
            Column[] foreignKeyColumnTable1 = new Column[foreignKeyCount];
            Column[] foreignKeyColumnTable2 = new Column[foreignKeyCount];
            ArrayList<String> foreignKeys = commonData.tableInfo(name).getForeignKeys();
            String[] foreignKeyTable = new String[foreignKeyCount];

            for (int j = 0; j < foreignKeyCount; j++) {
                foreignKeyColumnTable1[j] = col(foreignKeys.get(2*j));
                foreignKeyColumnTable2[j] = col(foreignKeys.get(2*j + 1));
                foreignKeyTable[j] = commonData.tableInfo(name).getForeignKeyTable().get(j);
            }

            Dataset<Row> newDataFrameKeys = dataFrame
                    .select(foreignKeyColumnTable1);

            if (foreignKeyCount == 1) {

                Dataset<Row> dataframeFromDB = spark
                        .read()
                        .jdbc("jdbc:mysql://localhost:3306", "warehouse." + foreignKeyTable[0], Initializer.connectionProperties())
                        .select(foreignKeyColumnTable2);

                validData = newDataFrameKeys
                        .intersect(dataframeFromDB)
                        .join(dataFrame, dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0))))
                        .drop(dataFrame.col(foreignKeys.get(0)));
            } else {

                Dataset<Row> dataframeFromDB1 = spark
                        .read()
                        .jdbc("jdbc:mysql://localhost:3306", "warehouse." + foreignKeyTable[0], Initializer.connectionProperties())
                        .select(foreignKeyColumnTable2[0]);

                Dataset<Row> dataframeFromDB2 = spark
                        .read()
                        .jdbc("jdbc:mysql://localhost:3306", "warehouse." + foreignKeyTable[1], Initializer.connectionProperties())
                        .select(foreignKeyColumnTable2[1]);

                Dataset<Row> dataframeFromDB = dataframeFromDB1.join(dataframeFromDB2);


                validData = newDataFrameKeys
                        .intersect(dataframeFromDB)
                        .join(dataFrame, (dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0)))
                                .$amp$amp(dataFrame.col(foreignKeys.get(2)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(2))))))
                        .drop(dataFrame.col(foreignKeys.get(0)))
                        .drop(dataFrame.col(foreignKeys.get(2)));
            }

            System.out.println("validDataForeignKeyCheck done");
            validData.show();
            return validData;
        }
        else {
            return(dataFrame);
        }
    }
    public Dataset<Row> invalidDataForeinKeyCheck(Dataset<Row> dataFrame) {
        CommonData commonData = new CommonData();
        int foreignKeyCount = commonData.tableInfo(name).getForeignKeys().size();

        Dataset<Row> invalidData = null;

        if (foreignKeyCount > 0){
            Column[] foreignKeyColumnTable1 = new Column[foreignKeyCount];
            Column[] foreignKeyColumnTable2 = new Column[foreignKeyCount];
            ArrayList<String> foreignKeys = commonData.tableInfo(name).getForeignKeys();
            String[] foreignKeyTable = new String[foreignKeyCount];

            for (int j = 0; j < foreignKeyCount; j++) {
                foreignKeyColumnTable1[j] = col(foreignKeys.get(2*j));
                foreignKeyColumnTable2[j] = col(foreignKeys.get(2*j + 1));
                foreignKeyTable[j] = commonData.tableInfo(name).getForeignKeyTable().get(j);
            }

            Dataset<Row> newDataFrameKeys = dataFrame
                    .select(foreignKeyColumnTable1);

            if (foreignKeyCount == 1) {

                Dataset<Row> dataframeFromDB = spark
                        .read()
                        .jdbc("jdbc:mysql://localhost:3306", "warehouse." + foreignKeyTable[0], Initializer.connectionProperties())
                        .select(foreignKeyColumnTable2);

                invalidData = newDataFrameKeys
                        .intersect(dataframeFromDB)
                        .join(dataFrame, dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0))))
                        .drop(dataFrame.col(foreignKeys.get(0)))
                        .withColumn("reject_reason",lit("foreign key violation"));
            } else {

                Dataset<Row> dataframeFromDB1 = spark
                        .read()
                        .jdbc("jdbc:mysql://localhost:3306", "warehouse." + foreignKeyTable[0], Initializer.connectionProperties())
                        .select(foreignKeyColumnTable2[0]);

                Dataset<Row> dataframeFromDB2 = spark
                        .read()
                        .jdbc("jdbc:mysql://localhost:3306", "warehouse." + foreignKeyTable[1], Initializer.connectionProperties())
                        .select(foreignKeyColumnTable2[1]);

                Dataset<Row> dataframeFromDB = dataframeFromDB1.join(dataframeFromDB2);


                invalidData = newDataFrameKeys
                        .except(dataframeFromDB)
                        .join(dataFrame, (dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0)))
                                .$amp$amp(dataFrame.col(foreignKeys.get(2)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(2))))))
                        .drop(dataFrame.col(foreignKeys.get(0)))
                        .drop(dataFrame.col(foreignKeys.get(2)))
                        .withColumn("reject_reason",lit("foreign key violation"));
            }

            System.out.println("invalidDataForeinKeyCheck done");
            invalidData.show();
        }
        return invalidData;
    }
}
