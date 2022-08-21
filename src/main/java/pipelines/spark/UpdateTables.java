package pipelines.spark;

import etl.spark.ExtractSpark;
import etl.spark.LoadSpark;
import etl.spark.TransformSpark;
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
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executePipeline");
        ExtractSpark extract = new ExtractSpark(spark, name, sizeFactor);
        Dataset<Row> newData = extract.extractFromCsv(true);

        TransformSpark transform = new TransformSpark(spark, name);
        LoadSpark load = new LoadSpark(name);

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