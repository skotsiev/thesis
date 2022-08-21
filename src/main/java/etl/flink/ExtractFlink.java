package etl.flink;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.concurrent.TimeUnit;

import static etl.common.Constants.ROOT_CSV_PATH;

public class ExtractFlink {

    public ExtractFlink(String name, String sizeFactor) {
        this.name = name;
        this.sizeFactor = sizeFactor;
    }

    final String name;
    final String sizeFactor;



    public Dataset<Row> extractFromCsv(){
        final String rootPath = ROOT_CSV_PATH;
        final String path = rootPath + sizeFactor + "/" + name + ".tbl";

        System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Import data from " + name + ".csv");
        System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Start reading data");
        long start = System.currentTimeMillis();

//        CsvReaderFormat<SomePojo> csvFormat = CsvReaderFormat.forPojo(SomePojo.class);
//        FileSource<SomePojo> source =
//                FileSource.forRecordStreamFormat(csvFormat, path).build();
        Dataset<Row> dataFrame= null;
        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        if (elapsedTime < 1000){
            System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Elapsed time to read " + dataFrame.count() + " lines: " + elapsedTime + " millis");
        }
            else {
            long elapsedTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
            System.out.println("[" + getClass().getSimpleName() + "]\t\t" + "Elapsed time to read: " + dataFrame.count() + " lines: " + elapsedTimeSeconds + " seconds");
        }
            return dataFrame;
    }
}
