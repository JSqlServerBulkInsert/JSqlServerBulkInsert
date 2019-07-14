// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.converters;

import java.math.BigInteger;

public class BigIntegerConverter extends BaseConverter<BigInteger> {
    @Override
    public Object internalConvert(BigInteger value) {
        return value.longValueExact();
    }
}
