package de.bytefish.jsqlserverbulkinsert.converters;

public class DoubleConverter extends BaseConverter<Double> {
    @Override
    public Object internalConvert(Double value) {
        return value;
    }
}
