package de.bytefish.jsqlserverbulkinsert.converters;

import java.time.OffsetTime;

public class OffsetTimeConverter extends BaseConverter<OffsetTime> {

    @Override
    public Object internalConvert(OffsetTime value) {
        return value;
    }
}
