package de.bytefish.jsqlserverbulkinsert.converters;

import java.time.OffsetDateTime;
import java.time.OffsetTime;

public class OffsetDateTimeConverter extends BaseConverter<OffsetDateTime> {

    @Override
    public Object internalConvert(OffsetDateTime value) {
        return value;
    }
}
