package spark.stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import spark.common.Queries;

import java.util.concurrent.TimeoutException;

public class NativeQueriesStream {
    static public void execute(SparkSession spark, String query) throws StreamingQueryException, TimeoutException {

        String q = Queries.hashMap.get(query);

        Dataset<Row> result = spark.sql(q);

        StreamingQuery streamingQuery = result
                .writeStream()
                .outputMode(OutputMode.Update())
                .format("console")
                .start();

        streamingQuery.awaitTermination();
    }
}
