// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.converters;

public abstract class BaseConverter<TSourceType> implements IConverter<TSourceType> {

    public Object convert(TSourceType value) {
        if(value == null) {
            return null;
        }

        return internalConvert(value);
    }

    public abstract Object internalConvert(TSourceType value);
}
