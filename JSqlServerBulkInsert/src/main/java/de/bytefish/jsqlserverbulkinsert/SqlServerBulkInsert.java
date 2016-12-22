// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import de.bytefish.jsqlserverbulkinsert.functional.Func2;
import de.bytefish.jsqlserverbulkinsert.model.ColumnDefinition;
import de.bytefish.jsqlserverbulkinsert.model.ColumnMetaData;
import de.bytefish.jsqlserverbulkinsert.model.TableDefinition;
import de.bytefish.jsqlserverbulkinsert.records.SqlServerRecordBuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.*;
import java.util.stream.Stream;

public abstract class SqlServerBulkInsert<TEntity> implements ISqlServerBulkInsert<TEntity> {

    private TableDefinition table;

    private List<ColumnDefinition<TEntity>> columns;

    public SqlServerBulkInsert(String schemaName, String tableName)
    {
        this.table = new TableDefinition(schemaName, tableName);
        this.columns = new ArrayList<>();
    }

    public void saveAll(Connection connection, Stream<TEntity> entities) throws SQLException {

        final SqlServerRecordBuilder<TEntity> sqlServerRecordBuilder = new SqlServerRecordBuilder<>(columns);

        try (SQLServerBulkCopy sqlServerBulkCopy = new SQLServerBulkCopy(connection)) {

            // The Destination Table to write to:
            sqlServerBulkCopy.setDestinationTableName(table.GetFullQualifiedTableName());

            entities
                    .map(x -> sqlServerRecordBuilder.build(x))
                    .forEach(x -> internalWriteToServer(sqlServerBulkCopy, x));
        }
    }

    public void internalWriteToServer(SQLServerBulkCopy sqlServerBulkCopy, ISQLServerBulkRecord record) {
        try {
            sqlServerBulkCopy.writeToServer(record);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void mapBoolean(String columnName, Func2<TEntity, Boolean> propertyGetter) {
        addColumn(columnName, Types.BOOLEAN, propertyGetter);
    }

    protected void mapNumeric(String columnName, int scale, int precision, Func2<TEntity, BigDecimal> propertyGetter)
    {
        addColumn(columnName, Types.NUMERIC, precision, scale, false, propertyGetter);
    }

    protected void mapDecimal(String columnName, int scale, int precision, Func2<TEntity, BigDecimal> propertyGetter)
    {
        // We need to scale the incoming decimal, before writing it to SQL Server:
        final Func2<TEntity, BigDecimal> wrapper = entity -> {
            BigDecimal result = propertyGetter.invoke(entity);

            return result.setScale(scale, RoundingMode.HALF_UP);
        };

        addColumn(columnName, Types.DECIMAL, precision, scale, false, wrapper);
    }

    protected void mapReal(String columnName, Func2<TEntity, Float> propertyGetter)
    {
        addColumn(columnName, Types.REAL, propertyGetter);
    }

    protected void mapBigInt(String columnName, Func2<TEntity, BigInteger> propertyGetter) {

        // SQL Server expects the Big Integer as a Long Value:
        final Func2<TEntity, Long> wrapper = entity -> {
            BigInteger resultAsBigInteger = propertyGetter.invoke(entity);

            return resultAsBigInteger.longValueExact();
        };

        addColumn(columnName, Types.BIGINT, wrapper);
    }

    protected void mapDate(String columnName, Func2<TEntity, LocalDate> propertyGetter) {
        addColumn(columnName, Types.DATE, propertyGetter);
    }

    protected void mapDouble(String columnName, Func2<TEntity, Double> propertyGetter)
    {
        addColumn(columnName, Types.DOUBLE, propertyGetter);
    }

    protected void mapSmallInt(String columnName, Func2<TEntity, String> propertyGetter)
    {
        addColumn(columnName, Types.SMALLINT, propertyGetter);
    }

    protected void mapTinyInt(String columnName, Func2<TEntity, String> propertyGetter)
    {
        addColumn(columnName, Types.TINYINT, propertyGetter);
    }

    protected void mapTimeWithTimeZone(String columnName, Func2<TEntity, OffsetTime> propertyGetter) {
        addColumn(columnName, 2013, propertyGetter);
    }

    protected void mapDateTimeWithTimeZone(String columnName, Func2<TEntity, OffsetDateTime> propertyGetter) {
        addColumn(columnName, 2014, propertyGetter);
    }

    private void addColumn(String name, int type, boolean isAutoIncrement, Func2<TEntity, Object> propertyGetter)
    {
        // Create the current Column Meta Data:
        ColumnMetaData columnMetaData = new ColumnMetaData(name, type, 0, 0, isAutoIncrement);

        // Add a new Column with the Meta Data and Property Getter:
        addColumn(columnMetaData, propertyGetter);
    }

    private <TProperty> void addColumn(String name, int type, Func2<TEntity, TProperty> propertyGetter)
    {
        // Create the current Column Meta Data:
        ColumnMetaData columnMetaData = new ColumnMetaData(name, type);

        // Add a new Column with the Meta Data and Property Getter:
        addColumn(columnMetaData, propertyGetter);
    }

    private <TProperty> void addColumn(String name, int type, int precision, int scale, boolean isAutoIncrement, Func2<TEntity, TProperty> propertyGetter)
    {
        // Create the current Column Meta Data:
        ColumnMetaData columnMetaData = new ColumnMetaData(name, type, precision, scale, isAutoIncrement);

        // Add a new Column with the Meta Data and Property Getter:

        addColumn(columnMetaData, propertyGetter);
    }

    private <TProperty> void addColumn(ColumnMetaData columnMetaData, Func2<TEntity, TProperty> propertyGetter)
    {
        // Add a new Column with the Meta Data and Property Getter:
        columns.add(new ColumnDefinition(columnMetaData, propertyGetter));
    }
}
