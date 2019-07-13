package de.bytefish.jsqlserverbulkinsert.converters;

public class FloatConverter extends BaseConverter<Float> {
    @Override
    public Object internalConvert(Float value) {
        return value;
    }
}
