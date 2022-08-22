package etl.delta;

import etl.common.TableInfo;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public class TransformDelta {

    public TransformDelta(SparkSession spark, String name) {
        this.spark = spark;
        this.name = name;
    }

    private final SparkSession spark;
    private final String name;


    public Dataset<Row> validDataPrimaryKeyCheck(Dataset<Row> dataFrame) {

        System.out.println("[" + getClass().getSimpleName() + "]\t" + "validDataPrimaryKeyCheck");
        TableInfo tableInfo = new TableInfo(name);
        int primaryKeyCount = tableInfo.getPrimaryKeys().size();

        Column[] primaryKeyColumn = new Column[primaryKeyCount];
        ArrayList<String> primaryKeys = tableInfo.getPrimaryKeys();

        for (int j = 0; j < primaryKeyCount; j++) {
            primaryKeyColumn[j] = col(primaryKeys.get(j));
        }

        Dataset<Row> dataframeFromDB = spark.read().format("delta")
                .load("/tmp/delta-" + name)
                .select(primaryKeyColumn);

        Dataset<Row> newDataFrameKeys = dataFrame
                .select(primaryKeyColumn);

        Dataset<Row> validData;

        System.out.println("[" + getClass().getSimpleName() + "]\t" + "primaryKeyCount: " + primaryKeyCount);
        if (primaryKeyCount == 1) {
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
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "validDataPrimaryKeyCheck done");
        validData.show();
        return validData;
    }

    public Dataset<Row> invalidDataPrimaryKeyCheck(Dataset<Row> dataFrame) {
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "invalidDataPrimaryKeyCheck");
        TableInfo tableInfo = new TableInfo(name);
        int primaryKeyCount = tableInfo.getPrimaryKeys().size();

        Column[] primaryKeyColumn = new Column[primaryKeyCount];
        ArrayList<String> primaryKeys = tableInfo.getPrimaryKeys();

        for (int j = 0; j < primaryKeyCount; j++) {
            primaryKeyColumn[j] = col(primaryKeys.get(j));
        }

        Dataset<Row> dataframeFromDB = spark.read().format("delta")
                .load("/tmp/delta-" + name)
                .select(primaryKeyColumn);

        Dataset<Row> newDataFrameKeys = dataFrame
                .select(primaryKeyColumn);

        Dataset<Row> invalidData;

        if (primaryKeyCount == 1) {
            invalidData = newDataFrameKeys
                    .intersect(dataframeFromDB)
                    .join(dataFrame, dataFrame.col(primaryKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(0))))
                    .drop(dataFrame.col(primaryKeys.get(0)))
                    .withColumn("reject_reason", lit("primary key violation"));

        } else {
            invalidData = newDataFrameKeys
                    .intersect(dataframeFromDB)
                    .join(dataFrame, (dataFrame.col(primaryKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(0)))
                            .$amp$amp(dataFrame.col(primaryKeys.get(1)).$eq$eq$eq(newDataFrameKeys.col(primaryKeys.get(1))))))
                    .drop(dataFrame.col(primaryKeys.get(0)))
                    .drop(dataFrame.col(primaryKeys.get(1)))
                    .withColumn("reject_reason", lit("primary key violation"));
        }
        System.out.println("invalidDataPrimaryKeyCheck done");
        invalidData.show();
        return invalidData;
    }

    public Dataset<Row> validDataForeignKeyCheck(Dataset<Row> dataFrame) {
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "validDataForeignKeyCheck");
        TableInfo tableInfo = new TableInfo(name);
        int foreignKeyCount = tableInfo.getForeignKeys().size() / 2;

        Dataset<Row> validData;
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "foreignKeyCount: " + foreignKeyCount);
        if (foreignKeyCount >= 1) {
            Column[] foreignKeyColumnTable1 = new Column[foreignKeyCount];
            Column[] foreignKeyColumnTable2 = new Column[foreignKeyCount];
            ArrayList<String> foreignKeys = tableInfo.getForeignKeys();
            String[] foreignKeyTable = new String[foreignKeyCount];

            for (int j = 0; j < foreignKeyCount; j++) {
                foreignKeyColumnTable1[j] = col(foreignKeys.get(2 * j));
                foreignKeyColumnTable2[j] = col(foreignKeys.get(2 * j + 1));
                foreignKeyTable[j] = tableInfo.getForeignKeyTable().get(j);
            }

            Dataset<Row> newDataFrameKeys = dataFrame
                    .select(foreignKeyColumnTable1);

            if (foreignKeyCount == 1) {

                Dataset<Row> dataframeFromDB = spark.read().format("delta")
                        .load("/tmp/delta-" + foreignKeyTable[0])
                        .select(foreignKeyColumnTable2);

                validData = newDataFrameKeys
                        .intersect(dataframeFromDB)
                        .join(dataFrame, dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0))))
                        .drop(dataFrame.col(foreignKeys.get(0)));
            } else {

                Dataset<Row> dataframeFromDB1 = spark.read().format("delta")
                        .load("/tmp/delta-" + foreignKeyTable[0])
                        .select(foreignKeyColumnTable2[0]);

                Dataset<Row> dataframeFromDB2 = spark.read().format("delta")
                        .load("/tmp/delta-" + foreignKeyTable[1])
                        .select(foreignKeyColumnTable2[1]);

                Dataset<Row> dataframeFromDB = dataframeFromDB1.join(dataframeFromDB2);

                validData = newDataFrameKeys
                        .intersect(dataframeFromDB)
                        .join(dataFrame, (dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0)))
                                .$amp$amp(dataFrame.col(foreignKeys.get(2)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(2))))))
                        .drop(dataFrame.col(foreignKeys.get(0)))
                        .drop(dataFrame.col(foreignKeys.get(2)));
            }

            System.out.println("[" + getClass().getSimpleName() + "]\t" + "validDataForeignKeyCheck done");
            validData.show();
            return validData;
        } else {
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "validDataForeignKeyCheck skipped");
            return (dataFrame);
        }
    }

    public Dataset<Row> invalidDataForeinKeyCheck(Dataset<Row> dataFrame) {
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "invalidDataForeinKeyCheck");
        TableInfo tableInfo = new TableInfo(name);
        int foreignKeyCount = tableInfo.getForeignKeys().size() / 2;

        Dataset<Row> invalidData;

        if (foreignKeyCount > 0) {
            Column[] foreignKeyColumnTable1 = new Column[foreignKeyCount];
            Column[] foreignKeyColumnTable2 = new Column[foreignKeyCount];
            ArrayList<String> foreignKeys = tableInfo.getForeignKeys();
            String[] foreignKeyTable = new String[foreignKeyCount];

            for (int j = 0; j < foreignKeyCount; j++) {
                foreignKeyColumnTable1[j] = col(foreignKeys.get(2 * j));
                foreignKeyColumnTable2[j] = col(foreignKeys.get(2 * j + 1));
                foreignKeyTable[j] = tableInfo.getForeignKeyTable().get(j);
            }

            Dataset<Row> newDataFrameKeys = dataFrame
                    .select(foreignKeyColumnTable1);

            if (foreignKeyCount == 1) {

                Dataset<Row> dataframeFromDB = spark.read().format("delta")
                        .load("/tmp/delta-" + foreignKeyTable[0])
                        .select(foreignKeyColumnTable2);

                invalidData = newDataFrameKeys
                        .intersect(dataframeFromDB)
                        .join(dataFrame, dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0))))
                        .drop(dataFrame.col(foreignKeys.get(0)))
                        .withColumn("reject_reason", lit("foreign key violation"));
            } else {

                Dataset<Row> dataframeFromDB1 = spark.read().format("delta")
                        .load("/tmp/delta-" + foreignKeyTable[0])
                        .select(foreignKeyColumnTable2[0]);

                Dataset<Row> dataframeFromDB2 = spark.read().format("delta")
                        .load("/tmp/delta-" + foreignKeyTable[1])
                        .select(foreignKeyColumnTable2[1]);

                Dataset<Row> dataframeFromDB = dataframeFromDB1.join(dataframeFromDB2);

                invalidData = newDataFrameKeys
                        .except(dataframeFromDB)
                        .join(dataFrame, (dataFrame.col(foreignKeys.get(0)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(0)))
                                .$amp$amp(dataFrame.col(foreignKeys.get(2)).$eq$eq$eq(newDataFrameKeys.col(foreignKeys.get(2))))))
                        .drop(dataFrame.col(foreignKeys.get(0)))
                        .drop(dataFrame.col(foreignKeys.get(2)))
                        .withColumn("reject_reason", lit("foreign key violation"));
            }

            System.out.println("[" + getClass().getSimpleName() + "]\t" + "invalidDataForeinKeyCheck done");
            invalidData.show();
            return invalidData;
        } else {
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "invalidDataForeinKeyCheck skipped");
            return dataFrame;
        }
    }
}
