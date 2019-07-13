package de.bytefish.jsqlserverbulkinsert.converters;

import java.sql.Timestamp;
import java.time.Instant;

public class InstantConverter extends BaseConverter<Instant> {

    @Override
    public Object internalConvert(Instant value) {
        Timestamp castedResult = new Timestamp(value.toEpochMilli());

        // Round to the nearest 100 nanoseconds, the precision that SQL server can handle:
        castedResult.setNanos((value.getNano()/100)*100);

        return castedResult;
    };

}
