// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.records;

import de.bytefish.jsqlserverbulkinsert.model.IColumnDefinition;

import java.util.List;

public class SqlServerRecordBuilder<TEntity> {

    private final List<IColumnDefinition<TEntity>> columns;

    public SqlServerRecordBuilder(List<IColumnDefinition<TEntity>> columns) {
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
