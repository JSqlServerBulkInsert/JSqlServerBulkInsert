package de.bytefish.jsqlserverbulkinsert.converters;

public class TinyIntConverter extends BaseConverter<Byte> {
    @Override
    public Object internalConvert(Byte value) {
        return value;
    }
}
