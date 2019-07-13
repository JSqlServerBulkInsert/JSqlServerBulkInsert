// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

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
