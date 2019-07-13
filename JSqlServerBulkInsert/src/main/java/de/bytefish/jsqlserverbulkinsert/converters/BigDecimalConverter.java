package de.bytefish.jsqlserverbulkinsert.converters;

import java.math.BigDecimal;

public class BigDecimalConverter extends BaseConverter<BigDecimal> {

    private final int scale;

    public BigDecimalConverter(int scale) {
        this.scale = scale;
    }

    @Override
    public Object internalConvert(BigDecimal value) {
        return value.setScale(scale, BigDecimal.ROUND_HALF_UP);
    }
}
