package etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.common.Initializer;

import static etl.CommonData.nation;

public class Transform {

    public static boolean validateDimensions(SparkSession spark, Dataset<Row> dataFrame, String name){
        String primaryKey = nation.get("pk");
        Dataset<Row> dataframeFromDB = spark.
                read()
                .jdbc("jdbc:mysql://localhost:3306", "warehouse." + name, Initializer.connectionProperties())
                .select(primaryKey);

        dataframeFromDB.show();
        dataFrame.show();
        return true;
    }
}
