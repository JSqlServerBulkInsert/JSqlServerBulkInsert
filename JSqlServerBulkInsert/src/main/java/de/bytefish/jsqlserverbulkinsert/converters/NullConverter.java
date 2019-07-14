package de.bytefish.jsqlserverbulkinsert.converters;

public class NullConverter<TPropertyType> extends BaseConverter<TPropertyType> {
    @Override
    public Object internalConvert(TPropertyType value) {
        return null;
    }
}
