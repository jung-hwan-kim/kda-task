package jungfly.kda.task;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class EventtimestampTest {
    static public void main(String[] args) {
        String t1 = "2020-01-04 15:13:18.025934";
        String t2 = t1 + "000Z";
        DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nX").withZone(ZoneId.of("UTC"));
        OffsetDateTime eventTime = OffsetDateTime.parse(t2, f);


        long eventTimeEpoch = eventTime.toEpochSecond();
        long eventTimeEpochMillis = eventTime.toInstant().toEpochMilli();
        System.out.println(eventTime + " -> " + eventTimeEpoch + " -> " + eventTimeEpochMillis);
        OffsetDateTime currTime = OffsetDateTime.now(ZoneId.of("UTC"));

        long currTimeEpoch = currTime.toEpochSecond();
        long currTimeEpochMillis = currTime.toInstant().toEpochMilli();
        System.out.println(currTime + " -> " + currTimeEpoch + " -> " + currTimeEpochMillis);
    }
}
