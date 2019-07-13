package de.bytefish.jsqlserverbulkinsert.converters;

import java.math.BigInteger;

public class BigIntegerConverter extends BaseConverter<BigInteger> {
    @Override
    public Object internalConvert(BigInteger value) {
        return value.longValueExact();
    }
}
