package etl.common;

import java.util.concurrent.TimeUnit;

public class Utils {

    public static String elapsedTime(long elapsedTime){
        String elapsedTimeString ;
        long minutes = TimeUnit.MILLISECONDS.toMinutes(elapsedTime);
        long remainingSeconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime) - TimeUnit.MINUTES.toSeconds(minutes);
        long remainingMillis = elapsedTime - TimeUnit.MINUTES.toMillis(minutes) - TimeUnit.SECONDS.toMillis(remainingSeconds);

        elapsedTimeString = String.format("\t%d min,\t%d sec,\t%d millis",
                minutes,
                remainingSeconds,
                remainingMillis
        );
        return elapsedTimeString;
    }
}
