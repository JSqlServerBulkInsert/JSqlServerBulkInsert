// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.converters;

import java.time.LocalDate;

public class LocalDateConverter extends BaseConverter<LocalDate> {
    @Override
    public Object internalConvert(LocalDate value) {
        return value;
    }
}
