package de.bytefish.jsqlserverbulkinsert.util;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.AbstractMap;

public class TimestampUtils {
    public AbstractMap.SimpleImmutableEntry<Long, Integer> convertUtcNanoToEpochSecAndNano(long value) {
        // loses subsecond data:
        long seconds = value / 1000000000;
        int nanoseconds = (int) (value - (seconds * 1000000000));

        // Round to 100 nanoseconds (precision that SQL server can handle):
        nanoseconds = (nanoseconds / 100) * 100;

        // Must include this adjustment to counteract the timezone adjustment that the SQL Server JDBC driver makes:
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(seconds, nanoseconds, ZoneOffset.UTC);
        long epochSeconds = localDateTime.toEpochSecond(OffsetDateTime.now().getOffset());

        return new AbstractMap.SimpleImmutableEntry<>(epochSeconds, nanoseconds);
    }
}