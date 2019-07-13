// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.converters;

import java.time.OffsetTime;

public class OffsetTimeConverter extends BaseConverter<OffsetTime> {

    @Override
    public Object internalConvert(OffsetTime value) {
        return value;
    }
}
