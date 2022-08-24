package pipelines.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static etl.common.Constants.ROOT_CSV_PATH;
import static etl.common.Schemas.createSchema;
import static pipelines.common.Initializer.initStream;

public class ContinuousUpdate {

    public ContinuousUpdate(SparkSession spark, String name) {
        this.spark = spark;
        this.name = name;
    }

    private final SparkSession spark;
    private final String name ;



    public void executePipeline() throws TimeoutException, StreamingQueryException {

        initStream(spark);

    }

}
