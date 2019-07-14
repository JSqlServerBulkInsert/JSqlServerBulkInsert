package de.bytefish.jsqlserverbulkinsert.converters;

public class IdentityConverter<TPropertyType> extends BaseConverter<TPropertyType> {
    @Override
    public Object internalConvert(TPropertyType value) {
        return value;
    }
}
