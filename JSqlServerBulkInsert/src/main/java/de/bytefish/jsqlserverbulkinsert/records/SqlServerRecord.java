// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.records;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import de.bytefish.jsqlserverbulkinsert.model.ColumnMetaData;
import de.bytefish.jsqlserverbulkinsert.model.IColumnDefinition;

import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SqlServerRecord<TEntity> implements ISQLServerBulkRecord {

    private final Set<Integer> columnOrdinals;

    private final Iterator<TEntity> entities;

    private final List<ColumnMetaData> columnMetaData;

    private final SqlServerRecordBuilder<TEntity> builder;

    @Override
    public void addColumnMetadata(int positionInSource, String name, int jdbcType, int precision, int scale, DateTimeFormatter dateTimeFormatter) throws SQLServerException {
        // We can safely ignore ...
    }

    @Override
    public void addColumnMetadata(int positionInSource, String name, int jdbcType, int precision, int scale) throws SQLServerException {
        // We can safely ignore ...
    }

    @Override
    public void setTimestampWithTimezoneFormat(String s) {
        // We can safely ignore ...
    }

    @Override
    public void setTimestampWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        // We can safely ignore ...
    }

    @Override
    public void setTimeWithTimezoneFormat(String s) {
        // We can safely ignore ...
    }

    @Override
    public void setTimeWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        // We can safely ignore ...
    }

    @Override
    public DateTimeFormatter getColumnDateTimeFormatter(int i) {
        // We don't need to implement it ...
        return null;
    }

    public SqlServerRecord(List<IColumnDefinition<TEntity>> columns, Iterator<TEntity> entities) {

        if(columns == null) {
            throw new IllegalArgumentException("columnDefinition");
        }

        if(entities == null) {
            throw new IllegalArgumentException("entities");
        }

        this.entities = entities;

        // Cache the Column Meta Data, so we don't calculate it for each Record:
        this.columnMetaData = columns.stream()
                .map(x -> x.getColumnMetaData())
                .collect(Collectors.toList());

        // Build the Object[] values Builder to populate the records faster:
        this.builder = new SqlServerRecordBuilder<>(columns);

        // Cache the Column Ordinals:
        this.columnOrdinals = IntStream
                .range(1, columnMetaData.size() + 1)
                .boxed()
                .collect(Collectors.toSet());
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        return columnOrdinals;
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
