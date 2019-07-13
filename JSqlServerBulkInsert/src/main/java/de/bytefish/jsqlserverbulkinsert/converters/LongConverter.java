package de.bytefish.jsqlserverbulkinsert.converters;

public class LongConverter extends BaseConverter<Long> {
    @Override
    public Object internalConvert(Long value) {
        return value;
    }
}
