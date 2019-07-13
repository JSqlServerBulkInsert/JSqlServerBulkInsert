package de.bytefish.jsqlserverbulkinsert.converters;

public class VarbinaryConverter extends BaseConverter<byte[]> {
    @Override
    public Object internalConvert(byte[] value) {
        return value;
    }
}
