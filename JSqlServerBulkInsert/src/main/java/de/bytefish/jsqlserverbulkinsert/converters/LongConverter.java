// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.converters;

public class LongConverter extends BaseConverter<Long> {
    @Override
    public Object internalConvert(Long value) {
        return value;
    }
}
