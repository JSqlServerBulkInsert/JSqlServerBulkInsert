// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.records;

import de.bytefish.jsqlserverbulkinsert.model.ColumnDefinition;
import de.bytefish.jsqlserverbulkinsert.model.ColumnMetaData;

import java.util.List;
import java.util.stream.Collectors;

public class SqlServerRecordBuilder<TEntity> {

    private final List<ColumnDefinition<TEntity>> columns;
    private final List<ColumnMetaData> columnMetaData;

    public SqlServerRecordBuilder(List<ColumnDefinition<TEntity>> columns) {
        if(columns == null) {
            throw new IllegalArgumentException("columns");
        }

        // Holds the Columns:
        this.columns = columns;

        // Cache the Column Meta Data, so we don't calculate it for each Record:
        this.columnMetaData = columns.stream()
                .map(x -> x.getColumnMetaData())
                .collect(Collectors.toList());
    }

    public SqlServerRecord build(final TEntity entity) {

        // Get the Values for each column:
        Object[] values = columns.stream()
                .map(x -> x.getPropertyValue(entity))
                .toArray();

        // And finally build the SqlServerRecord:
        return new SqlServerRecord(columnMetaData, values);

    }

}
