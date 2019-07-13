package de.bytefish.jsqlserverbulkinsert.converters;

import java.sql.Timestamp;

public class TimestampConverter extends BaseConverter<Timestamp> {
    @Override
    public Object internalConvert(Timestamp value) {
        return value;
    }
}
