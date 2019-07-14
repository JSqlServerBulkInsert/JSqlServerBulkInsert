// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.converters;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class BigDecimalConverter extends BaseConverter<BigDecimal> {

    private final int scale;
    private final RoundingMode roundingMode;

    public BigDecimalConverter(int scale, RoundingMode roundingMode) {
        this.roundingMode = roundingMode;
        this.scale = scale;
    }

    @Override
    public Object internalConvert(BigDecimal value) {
        return value.setScale(scale, BigDecimal.ROUND_HALF_UP);
    }
}
