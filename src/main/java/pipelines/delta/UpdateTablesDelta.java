package pipelines.delta;

import etl.delta.ExtractDelta;
import etl.delta.LoadDelta;
import etl.delta.TransformDelta;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import pipelines.spark.UpdateTables;

import static etl.common.Utils.elapsedTimeSeconds;

public class UpdateTablesDelta {
    final static Logger logger = LogManager.getLogger(UpdateTables.class);
    public UpdateTablesDelta(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name;
    private final String sizeFactor;

    public void executePipeline(){
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executePipeline");
        logger.info("=======================[" + getClass().getSimpleName() + "]=======================");
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Starting pipeline execution");
        long start = System.currentTimeMillis();

        ExtractDelta extract = new ExtractDelta(spark, name, sizeFactor);
        Dataset<Row> newData = extract.extractFromCsv(true);

        TransformDelta transform = new TransformDelta(spark, name, sizeFactor);
        LoadDelta load = new LoadDelta(name, sizeFactor);

        Dataset<Row> validPrimaryKeyData = transform.validDataPrimaryKeyCheck(newData);
        Dataset<Row> validData = transform.validDataForeignKeyCheck(validPrimaryKeyData);
        Dataset<Row> invalidPrimaryKeyData = transform.invalidDataPrimaryKeyCheck(newData);
//        Dataset<Row> invalidPrimaryKeyData = newData.except(validPrimaryKeyData).withColumn("reject_reason", lit("primary key violation"));
        Dataset<Row> invalidForeignKeyData = transform.invalidDataForeinKeyCheck(validPrimaryKeyData);
//        Dataset<Row> invalidForeignKeyData = validPrimaryKeyData.except(validData).withColumn("reject_reason", lit("foreign key violation"));


        if (invalidPrimaryKeyData.count() != 0 ){
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "invalidPrimaryKeyData");
            load.appendToDelta(invalidPrimaryKeyData,false);
        }
        else{
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "No invalidPrimaryKeyData");
        }
        if (invalidForeignKeyData.count() != 0 ){
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "invalidForeignKeyData");
            load.appendToDelta(invalidForeignKeyData,false);
        }
        else{
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "No invalidForeignKeyData");
        }
        if (validData.count() != 0 ){
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "validData");
            load.appendToDelta(validData,true);
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
        ExtractDelta extract = new ExtractDelta(spark, name, sizeFactor);
        Dataset<Row> newData = extract.multipleUpdate(index);

        LoadDelta load = new LoadDelta(name, sizeFactor);
        load.appendToDelta(newData,true);
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTimeSeconds(elapsedTime);
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executePipeline time: " + elapsedTimeString);
    }
}
