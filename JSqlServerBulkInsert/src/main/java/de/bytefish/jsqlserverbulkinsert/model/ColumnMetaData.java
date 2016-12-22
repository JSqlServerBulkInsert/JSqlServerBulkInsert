// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.model;

public class ColumnMetaData {

    private final String name;
    private final int type;
    private final int precision;
    private final int scale;
    private final boolean isAutoIncrement;

    public ColumnMetaData(String name, int type) {
        this(name, type, 0, 0, false);
    }

    public ColumnMetaData(String name, int type, int precision, int scale, boolean isAutoIncrement) {
        this.name = name;
        this.type = type;
        this.precision = precision;
        this.scale = scale;
        this.isAutoIncrement = isAutoIncrement;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public boolean isAutoIncrement() {
        return isAutoIncrement;
    }
}
