package etl.common;

import java.util.concurrent.TimeUnit;

public class Utils {

    public static String elapsedTimeSeconds(long elapsedTime){
        String elapsedTimeString ;
        long seconds = TimeUnit.MILLISECONDS.toSeconds(elapsedTime);
        long remainingMillis = elapsedTime - TimeUnit.SECONDS.toMillis(seconds);

        elapsedTimeString = String.format("\t%d sec,\t%d millis",
                seconds,
                remainingMillis
        );
        return elapsedTimeString;
    }

    public static String elapsedTimeMinutes(long elapsedTime){
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
