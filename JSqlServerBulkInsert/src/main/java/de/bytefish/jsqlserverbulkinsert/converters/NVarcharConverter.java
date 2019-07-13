package de.bytefish.jsqlserverbulkinsert.converters;

public class NVarcharConverter extends BaseConverter<String> {
    @Override
    public Object internalConvert(String value) {
        return value;
    }
}
