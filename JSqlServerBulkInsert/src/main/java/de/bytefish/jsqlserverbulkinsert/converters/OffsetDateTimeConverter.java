// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.converters;

import java.time.OffsetDateTime;

public class OffsetDateTimeConverter extends BaseConverter<OffsetDateTime> {

    @Override
    public Object internalConvert(OffsetDateTime value) {
        return value;
    }
}
