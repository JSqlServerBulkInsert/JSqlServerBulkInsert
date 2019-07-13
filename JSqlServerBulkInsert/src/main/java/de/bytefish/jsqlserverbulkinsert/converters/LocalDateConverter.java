package de.bytefish.jsqlserverbulkinsert.converters;

import java.time.LocalDate;

public class LocalDateConverter extends BaseConverter<LocalDate> {
    @Override
    public Object internalConvert(LocalDate value) {
        return value;
    }
}
