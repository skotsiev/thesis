package pipelines;

import etl.Extract;
import etl.Load;
import etl.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;

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
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executePipeline");
        Extract extract = new Extract(spark, name, sizeFactor);
        Dataset<Row> newData = extract.extractFromCsv();

        Transform transform = new Transform(spark, name);
        Load load = new Load(name);

        Dataset<Row> validPrimaryKeyData = transform.validDataPrimaryKeyCheck(newData);
        Dataset<Row> validData = transform.validDataForeignKeyCheck(validPrimaryKeyData);
        Dataset<Row> invalidPrimaryKeyData = transform.invalidDataPrimaryKeyCheck(newData);
//        Dataset<Row> invalidPrimaryKeyData = newData.except(validPrimaryKeyData).withColumn("reject_reason", lit("primary key violation"));
        Dataset<Row> invalidForeignKeyData = transform.invalidDataForeinKeyCheck(validPrimaryKeyData);
//        Dataset<Row> invalidForeignKeyData = validPrimaryKeyData.except(validData).withColumn("reject_reason", lit("foreign key violation"));


        if (invalidPrimaryKeyData.count() != 0 ){
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "invalidPrimaryKeyData");
            load.appendToMysql(invalidPrimaryKeyData,false);
        }
        else{
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "No invalidPrimaryKeyData");
        }
        if (invalidForeignKeyData.count() != 0 ){
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "invalidForeignKeyData");
            load.appendToMysql(invalidForeignKeyData,false);
        }
        else{
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "No invalidForeignKeyData");
        }
        if (validData.count() != 0 ){
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "validData");
            load.appendToMysql(validData,true);
        }
        else{
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "No validData");
        }
    }
}
