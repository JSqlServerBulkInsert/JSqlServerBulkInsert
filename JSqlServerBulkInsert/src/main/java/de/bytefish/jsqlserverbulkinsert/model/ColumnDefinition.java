// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.model;

import de.bytefish.jsqlserverbulkinsert.functional.Func2;

public class ColumnDefinition<TEntityType>
{
    private final ColumnMetaData columnMetaData;
    private final Func2<TEntityType, Object> propertyGetter;

    public ColumnDefinition(ColumnMetaData columnMetaData, Func2<TEntityType, Object> propertyGetter) {

        if(columnMetaData == null) {
            throw new IllegalArgumentException("columnMetaData");
        }

        if(propertyGetter == null) {
            throw new IllegalArgumentException("propertyGetter");
        }

        this.columnMetaData = columnMetaData;
        this.propertyGetter = propertyGetter;
    }

    public ColumnMetaData getColumnMetaData() {
        return columnMetaData;
    }

    public Object getPropertyValue(TEntityType entity) {
        return propertyGetter.invoke(entity);
    }

    @Override
    public String toString()
    {
        return String.format("ColumnDefinition (ColumnMetaData = {%1$s}, Serialize = {%2$s})", columnMetaData, propertyGetter);
    }
}