// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.converters;

import java.sql.Timestamp;

public class TimestampConverter extends BaseConverter<Timestamp> {
    @Override
    public Object internalConvert(Timestamp value) {
        return value;
    }
}
