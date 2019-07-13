package de.bytefish.jsqlserverbulkinsert.converters;

public class IntegerConverter extends BaseConverter<Integer> {
    @Override
    public Object internalConvert(Integer value) {
        return value;
    }
}
