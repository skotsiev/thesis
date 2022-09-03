package pipelines.delta;

import etl.delta.ExtractDelta;
import etl.delta.LoadDelta;
import etl.delta.TransformDelta;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static etl.common.Utils.elapsedTime;

public class UpdateTablesDelta2 {
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

        ExtractDelta extract = new ExtractDelta(spark, name, sizeFactor);
        Dataset<Row> newData = extract.extractFromCsv(true);
        System.out.println("newData.count(): " + newData.count());

        TransformDelta transform = new TransformDelta(spark, name, sizeFactor);

        transform.validDataPrimaryKeyCheckDeltaTable(newData);
        Dataset<Row> finalData = DeltaTable.forPath(spark, "/tmp/delta-" + name + sizeFactor).toDF();
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
        String elapsedTimeString = elapsedTime(elapsedTime);
        System.out.println("[" + getClass().getSimpleName() + "]\t" + "executePipeline time: " + elapsedTimeString);
    }
}
