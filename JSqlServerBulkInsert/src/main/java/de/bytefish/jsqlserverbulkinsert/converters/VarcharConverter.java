package de.bytefish.jsqlserverbulkinsert.converters;

public class VarcharConverter extends BaseConverter<String> {
    @Override
    public Object internalConvert(String value) {
        return value;
    }
}
