package jungfly.kda.task;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class EventtimestampParser {
    static final private DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nX").withZone(ZoneId.of("UTC"));
    static final private String ADJUSTER = "000Z";

    public static OffsetDateTime toDatetime(String eventtimestamp) {
        return OffsetDateTime.parse(eventtimestamp + ADJUSTER, FORMATTER);
    }

    public static Instant toInstant(String eventtimestamp) {
        return toDatetime(eventtimestamp).toInstant();
    }

    public static long toEpochMillis(String eventtimestamp) {
        return toInstant(eventtimestamp).toEpochMilli();
    }

}
