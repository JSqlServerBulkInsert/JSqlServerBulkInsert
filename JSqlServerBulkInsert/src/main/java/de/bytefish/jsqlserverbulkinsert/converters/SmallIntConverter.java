package de.bytefish.jsqlserverbulkinsert.converters;

public class SmallIntConverter extends BaseConverter<Short> {
    @Override
    public Object internalConvert(Short value) {
        return value;
    }
}
