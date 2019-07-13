// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.model;

import de.bytefish.jsqlserverbulkinsert.converters.IConverter;

import java.util.function.Function;


public class ColumnDefinition<TEntityType, TPropertyType> implements IColumnDefinition<TEntityType>
{
    private final ColumnMetaData columnMetaData;
    private final Function<TEntityType, TPropertyType> propertyGetter;
    private final IConverter<TPropertyType> converter;

    public ColumnDefinition(ColumnMetaData columnMetaData, Function<TEntityType, TPropertyType> propertyGetter, IConverter<TPropertyType> converter) {

        if(columnMetaData == null) {
            throw new IllegalArgumentException("columnMetaData");
        }

        if(propertyGetter == null) {
            throw new IllegalArgumentException("propertyGetter");
        }

        if(converter == null) {
            throw new IllegalArgumentException("converter");
        }

        this.columnMetaData = columnMetaData;
        this.propertyGetter = propertyGetter;
        this.converter = converter;
    }

    @Override
    public ColumnMetaData getColumnMetaData() {
        return columnMetaData;
    }

    @Override
    public Object getPropertyValue(TEntityType entity) {
        TPropertyType value = propertyGetter.apply(entity);

        return converter.convert(value);
    }

    @Override
    public String toString()
    {
        return String.format("ColumnDefinition (ColumnMetaData = {%1$s}, Serialize = {%2$s})", columnMetaData, propertyGetter);
    }
}