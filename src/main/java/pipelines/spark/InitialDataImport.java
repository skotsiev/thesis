package pipelines.spark;

import etl.spark.ExtractSpark;
import etl.spark.LoadSpark;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

import static etl.common.Constants.tableList;
import static etl.common.Utils.elapsedTimeSeconds;

public class InitialDataImport{

    final static Logger logger = LogManager.getLogger(InitialDataImport.class);
    public InitialDataImport(SparkSession spark, String name, String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name;
    private final String sizeFactor;

    public void executePipeline(){
        logger.info("=======================[" + getClass().getSimpleName() + "]=======================");
        logger.info("=================================================================");
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Starting pipeline execution");
        logger.info("-----------------------------------------------------------------");
        logger.info("[" + getClass().getSimpleName() + "]\t" + "sizeFactor " + sizeFactor);
        logger.info("-----------------------------------------------------------------");
        long start = System.currentTimeMillis();

        if (name.equals("all")){
            ArrayList<String> tableList = tableList();

            for(String table : tableList ) {
                long startLoop = System.currentTimeMillis();
                execute(table);
                long endLoop = System.currentTimeMillis();
                long elapsedTimeLoop = endLoop - startLoop;
                String elapsedTimeString = elapsedTimeSeconds(elapsedTimeLoop);
                System.out.println("[" + getClass().getSimpleName() + "]\t" + "Total time to import: " + table + ":" + elapsedTimeString);
                logger.info("[" + getClass().getSimpleName() + "]\t" + "Total time to import " + table + ":" + elapsedTimeString);
            }
            long end = System.currentTimeMillis();
            long elapsedTime = end - start;
            String elapsedTimeString = elapsedTimeSeconds(elapsedTime);
            System.out.println("[" + getClass().getSimpleName() + "]\t" + "Pipeline elapsed time: " + elapsedTimeString);
            logger.info("[" + getClass().getSimpleName() + "]\t" + "Pipeline elapsed time: " + elapsedTimeString);
        }
        else{
            execute(name);
        }
        logger.info("[" + getClass().getSimpleName() + "]\t" + "Pipeline execution complete");
        logger.info("=================================================================");
    }

    private void execute(String name){
        ExtractSpark extract = new ExtractSpark(spark, name, sizeFactor);
        Dataset<Row> data = extract.extractFromCsv(false);

        if(data.count() != 0){
            LoadSpark load = new LoadSpark(name, sizeFactor );
            load.overwriteToMysql(data, "warehouse"+sizeFactor);
        }
    }
}
