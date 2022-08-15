package etl.pipeline;

import etl.functions.Extract;
import etl.functions.Load;
import etl.functions.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UpdateTables {

    public UpdateTables(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name;
    private final String sizeFactor;

    public void executePipeline(){
        Extract extract = new Extract(spark, name, sizeFactor);
        Dataset<Row> newData = extract.extractFromCsv();

        Transform transform = new Transform(spark,name);
        Dataset<Row> validPrimaryKeyData = transform.validDataPrimaryKeyCheck(newData);
        Dataset<Row> validData = transform.validDataForeignKeyCheck(validPrimaryKeyData);

        Dataset<Row> invalidPrimaryKeyData = transform.invalidDataPrimaryKeyCheck(newData);
        Dataset<Row> invalidForeignKeyData = transform.invalidDataForeinKeyCheck(validPrimaryKeyData);

        Load load = new Load(name);
        if (validData!=null){
            load.appendToMysql(validData,true);
        }
        if (invalidPrimaryKeyData!=null){
            load.appendToMysql(invalidPrimaryKeyData,false);
        }
        if (invalidForeignKeyData!=null){
            load.appendToMysql(invalidForeignKeyData,false);
        }
    }
}
