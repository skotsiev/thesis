package pipelines.spark;

import etl.spark.ExtractSpark;
import etl.spark.LoadSpark;
import etl.spark.TransformSpark;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static etl.common.Utils.elapsedTimeSeconds;

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
        newData.show();
        long endRead = System.currentTimeMillis();
        long elapsedTimeRead = endRead - start;
        String elapsedTimeReadString = elapsedTimeSeconds(elapsedTimeRead);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Elapsed reading time:\t" + elapsedTimeReadString);

        long startValidation = System.currentTimeMillis();
        TransformSpark transform = new TransformSpark(spark, name, sizeFactor);
        LoadSpark load = new LoadSpark(name, sizeFactor);

        Dataset<Row> validPrimaryKeyData = transform.validDataPrimaryKeyCheck(newData);
//        Dataset<Row> validData = transform.validDataForeignKeyCheck(validPrimaryKeyData);
//        Dataset<Row> invalidPrimaryKeyData = transform.invalidDataPrimaryKeyCheck(newData);
//        Dataset<Row> invalidPrimaryKeyData = newData.except(validPrimaryKeyData).withColumn("reject_reason", lit("primary key violation"));
//        Dataset<Row> invalidForeignKeyData = transform.invalidDataForeignKeyCheck(validPrimaryKeyData);
//        Dataset<Row> invalidForeignKeyData = validPrimaryKeyData.except(validData).withColumn("reject_reason", lit("foreign key violation"));
//        System.out.println("invalidPrimaryKeyData"+invalidPrimaryKeyData.count());
        long endValidation = System.currentTimeMillis();
        long elapsedTimeValidation = endValidation - startValidation;
        String elapsedTimeValidationString = elapsedTimeSeconds(elapsedTimeValidation);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Elapsed validation time: " + elapsedTimeValidationString);


//        if (invalidPrimaryKeyData.count() != 0 ){
//            System.out.println("[" + getClass().getSimpleName() + "]\t" + "invalidPrimaryKeyData");
//            load.appendToMysql(invalidPrimaryKeyData,false);
//        }
//        else{
//            System.out.println("[" + getClass().getSimpleName() + "]\t" + "No invalidPrimaryKeyData");
//        }
//        if (invalidForeignKeyData.count() != 0 ){
//            System.out.println("[" + getClass().getSimpleName() + "]\t" + "invalidForeignKeyData");
//            load.appendToMysql(invalidForeignKeyData,false);
//        }
//        else{
//            System.out.println("[" + getClass().getSimpleName() + "]\t" + "No invalidForeignKeyData");
//        }
        if (validPrimaryKeyData.count() != 0 ){
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "validData");
            load.appendToMysql(validPrimaryKeyData,true);
        }
        else{
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "No validData");
        }
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTimeSeconds(elapsedTime);
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "Total elapsed time: " + elapsedTimeString);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Total elapsed time: " + elapsedTimeString);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Pipeline execution complete");
        logger.info("============================================================");
    }

    public void executePipeline(int index){
        System.out.println("[" + getClass().getSimpleName() + "]");
        long start = System.currentTimeMillis();

        ExtractSpark extract = new ExtractSpark(spark, name, sizeFactor);
        Dataset<Row> newData = extract.multipleUpdate(index);

        LoadSpark load = new LoadSpark(name, sizeFactor);
        load.appendToMysql(newData,true);
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTimeSeconds(elapsedTime);
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executePipeline time: " + elapsedTimeString);

    }
}