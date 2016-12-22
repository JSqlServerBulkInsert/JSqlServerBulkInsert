// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.records;

import de.bytefish.jsqlserverbulkinsert.model.ColumnDefinition;
import de.bytefish.jsqlserverbulkinsert.model.ColumnMetaData;

import java.util.List;
import java.util.stream.Collectors;

public class SqlServerRecordBuilder<TEntity> {

    private final List<ColumnDefinition<TEntity>> columns;

    public SqlServerRecordBuilder(List<ColumnDefinition<TEntity>> columns) {
        if(columns == null) {
            throw new IllegalArgumentException("columns");
        }
        this.columns = columns;
    }

    public Object[] build(final TEntity entity) {
        // Get the Values for each column:
        return columns.stream()
                .map(x -> x.getPropertyValue(entity))
                .toArray();
    }

}
