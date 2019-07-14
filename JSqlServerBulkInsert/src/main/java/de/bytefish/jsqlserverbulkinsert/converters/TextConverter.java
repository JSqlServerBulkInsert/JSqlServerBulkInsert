// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.converters;

public class TextConverter extends BaseConverter<Character> {
    @Override
    public Object internalConvert(Character value) {
        return value;
    }
}
