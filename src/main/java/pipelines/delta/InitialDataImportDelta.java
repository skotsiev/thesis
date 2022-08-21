package pipelines.delta;

import etl.delta.ExtractDelta;
import etl.delta.LoadDelta;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;

import static etl.common.Constants.tableList;

public class InitialDataImportDelta {

    public InitialDataImportDelta(SparkSession spark, String name , String sizeFactor) {
        this.spark = spark;
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    private final SparkSession spark;
    private final String name;
    private final String sizeFactor;

    public void executePipeline(){
        if (name.equals("all")){
            ArrayList<String> tableList = tableList();

            for(String i : tableList ) {
                extractFromCsv(i);
            }
        }
        else{
            extractFromCsv(name);
        }
    }

    private void extractFromCsv(String name){
        ExtractDelta extract = new ExtractDelta(spark, name, sizeFactor);
        Dataset<Row> data = extract.extractFromCsv(false);

        if(data.count() != 0){
            LoadDelta load = new LoadDelta(name);
            load.overwriteToDelta(spark, data);
        }
    }
}
