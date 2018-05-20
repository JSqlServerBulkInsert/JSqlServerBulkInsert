// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.records;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import de.bytefish.jsqlserverbulkinsert.model.ColumnDefinition;
import de.bytefish.jsqlserverbulkinsert.model.ColumnMetaData;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SqlServerRecord<TEntity> implements ISQLServerBulkRecord {

    private final Iterator<TEntity> entities;

    private final List<ColumnMetaData> columnMetaData;

    private final SqlServerRecordBuilder<TEntity> builder;

    public SqlServerRecord(List<ColumnDefinition<TEntity>> columnDefinition, Iterator<TEntity> entities) {
        if(columnDefinition == null) {
            throw new IllegalArgumentException("columnDefinition");
        }

        if(entities == null) {
            throw new IllegalArgumentException("entities");
        }

        this.entities = entities;

        // Cache the Column Meta Data, so we don't calculate it for each Record:
        this.columnMetaData = columnDefinition.stream()
                .map(x -> x.getColumnMetaData())
                .collect(Collectors.toList());

        // Cache a Values Builder to populate Records faster:
        this.builder = new SqlServerRecordBuilder<TEntity>(columnDefinition);
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        // Simply return the length of the List:
        return IntStream
                .range(1, columnMetaData.size() + 1)
                .boxed()
                .collect(Collectors.toSet());
    }

    @Override
    public String getColumnName(int i) {
        return columnMetaData.get(i-1).getName();
    }

    @Override
    public int getColumnType(int i) {
        return columnMetaData.get(i-1).getType();
    }

    @Override
    public int getPrecision(int i) {
        return columnMetaData.get(i-1).getPrecision();
    }

    @Override
    public int getScale(int i) {
        return columnMetaData.get(i-1).getScale();
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return columnMetaData.get(i-1).isAutoIncrement();
    }

    @Override
    public Object[] getRowData() throws SQLServerException {
        TEntity entity = entities.next();

        return builder.build(entity);
    }

    @Override
    public boolean next() throws SQLServerException {
        return entities.hasNext();
    }
}
