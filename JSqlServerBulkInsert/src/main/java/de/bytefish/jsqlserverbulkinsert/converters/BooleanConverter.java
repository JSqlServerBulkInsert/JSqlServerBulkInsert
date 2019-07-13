package de.bytefish.jsqlserverbulkinsert.converters;

public class BooleanConverter extends BaseConverter<Boolean> {
    @Override
    public Object internalConvert(Boolean value) {
        return value;
    }
}
