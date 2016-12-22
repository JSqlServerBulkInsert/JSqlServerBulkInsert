// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.records;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import de.bytefish.jsqlserverbulkinsert.model.ColumnMetaData;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SqlServerRecord implements ISQLServerBulkRecord {

    private final List<ColumnMetaData> columnMetaData;
    private final Object[] values;

    public SqlServerRecord(List<ColumnMetaData> columnMetaData, Object[] values) {
        if(columnMetaData == null) {
            throw new IllegalArgumentException("columnMetaData");
        }
        if(values == null) {
            throw new IllegalArgumentException("values");
        }
        this.columnMetaData = columnMetaData;
        this.values = values;
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        // Simply return the length of the List:
        return IntStream
                .range(0, columnMetaData.size())
                .boxed()
                .collect(Collectors.toSet());
    }

    @Override
    public String getColumnName(int i) {
        return columnMetaData.get(i).getName();
    }

    @Override
    public int getColumnType(int i) {
        return columnMetaData.get(i).getType();
    }

    @Override
    public int getPrecision(int i) {
        return columnMetaData.get(i).getPrecision();
    }

    @Override
    public int getScale(int i) {
        return columnMetaData.get(i).getScale();
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return columnMetaData.get(i).isAutoIncrement();
    }

    @Override
    public Object[] getRowData() throws SQLServerException {
        return values;
    }

    @Override
    public boolean next() throws SQLServerException {
        return false;
    }
}
