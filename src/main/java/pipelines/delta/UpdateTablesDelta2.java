package pipelines.delta;

import etl.delta.ExtractDelta;
import etl.delta.LoadDelta;
import etl.delta.TransformDelta;
import io.delta.tables.DeltaTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static etl.common.Utils.elapsedTimeSeconds;

public class UpdateTablesDelta2 {
    final static Logger logger = LogManager.getLogger(UpdateTablesDelta2.class);
    public UpdateTablesDelta2(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name;
    private final String sizeFactor;

    public void executePipeline() {
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executePipeline");
        logger.info("=======================[" + getClass().getSimpleName() + "]=======================");
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Starting pipeline execution");
        long start = System.currentTimeMillis();

        ExtractDelta extract = new ExtractDelta(spark, name, sizeFactor);
        Dataset<Row> newData = extract.extractFromCsv(true);
        long endRead = System.currentTimeMillis();
        long elapsedTimeRead = endRead - start;
        String elapsedTimeReadString = elapsedTimeSeconds(elapsedTimeRead);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Elapsed reading time: " + elapsedTimeReadString);
        Dataset<Row> startdata = DeltaTable.forPath(spark, "/tmp/delta-" + name + sizeFactor).toDF();
        System.out.println("startdata.count(): " + startdata.count());

        System.out.println("newData.count(): " + newData.count());
        long startTranform = System.currentTimeMillis();
        TransformDelta transform = new TransformDelta(spark, name, sizeFactor);

        transform.validDataPrimaryKeyCheckDeltaTable(newData);
        Dataset<Row> finalData = DeltaTable.forPath(spark, "/tmp/delta-" + name + sizeFactor).toDF();
        long end = System.currentTimeMillis();
        long elapsedTimeTranform = end - startTranform;
        String elapsedTimeTranformString = elapsedTimeSeconds(elapsedTimeTranform);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Elapsed validation + write time: " + elapsedTimeTranformString);




        long endend = System.currentTimeMillis();
        long elapsedTime = endend - start;
        String elapsedTimeString = elapsedTimeSeconds(elapsedTime);
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "Total elapsed time: " + elapsedTimeString);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Total elapsed time: " + elapsedTimeString);
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Pipeline execution complete");
        logger.info("============================================================");

        System.out.println("finalData.count(): " + finalData.count());
    }

    public void executePipeline(int index) {
        System.out.println("[" + getClass().getSimpleName() + "]");
        long start = System.currentTimeMillis();
        ExtractDelta extract = new ExtractDelta(spark, name, sizeFactor);
        Dataset<Row> newData = extract.multipleUpdate(index);

        LoadDelta load = new LoadDelta(name, sizeFactor);
        load.appendToDelta(newData, true);
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        String elapsedTimeString = elapsedTimeSeconds(elapsedTime);
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executePipeline time: " + elapsedTimeString);
    }
}
