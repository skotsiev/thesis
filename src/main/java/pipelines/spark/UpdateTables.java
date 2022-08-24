package pipelines.spark;

import etl.spark.ExtractSpark;
import etl.spark.LoadSpark;
import etl.spark.TransformSpark;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static etl.common.Utils.elapsedTime;

public class UpdateTables {
    final static Logger logger = LogManager.getLogger(UpdateTables.class);

    public UpdateTables(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name;
    private final String sizeFactor;

    public void executePipeline(){
        logger.info("=======================[" + getClass().getSimpleName() + "]=======================");
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Starting pipeline execution");
        long start = System.currentTimeMillis();

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

        long endValidation = System.currentTimeMillis();
        long elapsedTimeValidation = endValidation - start;
        String elapsedTimeValidationString = elapsedTime(elapsedTimeValidation);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Elapsed validation time: " + elapsedTimeValidationString);


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

        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTime(elapsedTime);
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "Total elapsed time: " + elapsedTimeString);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Total elapsed time: " + elapsedTimeString);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Pipeline execution complete");
        logger.info("============================================================");

    }
}