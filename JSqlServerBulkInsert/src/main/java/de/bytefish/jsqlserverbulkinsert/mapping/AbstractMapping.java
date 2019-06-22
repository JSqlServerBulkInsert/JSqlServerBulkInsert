// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.mapping;

import de.bytefish.jsqlserverbulkinsert.functional.Func2;
import de.bytefish.jsqlserverbulkinsert.model.ColumnDefinition;
import de.bytefish.jsqlserverbulkinsert.model.ColumnMetaData;
import de.bytefish.jsqlserverbulkinsert.model.TableDefinition;
import de.bytefish.jsqlserverbulkinsert.util.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class AbstractMapping<TEntity> {

    private TableDefinition table;

    private List<ColumnDefinition<TEntity>> columns;

    public AbstractMapping(String tableName)
    {
        this("", tableName);
    }

    public AbstractMapping(String schemaName, String tableName)
    {
        this.table = new TableDefinition(schemaName, tableName);
        this.columns = new ArrayList<>();
    }

    protected void mapBoolean(String columnName, Func2<TEntity, Boolean> propertyGetter) {
        addColumn(columnName, Types.BIT, propertyGetter);
    }

    protected void mapNumeric(String columnName, int precision, int scale, Func2<TEntity, BigDecimal> propertyGetter) {

        // We need to scale the incoming decimal, before writing it to SQL Server:
        final Func2<TEntity, BigDecimal> wrapper = entity -> {

            BigDecimal result = Optional.ofNullable(propertyGetter
                    .invoke(entity))
                    .map(d -> d.setScale(scale, BigDecimal.ROUND_HALF_UP))
                    .orElse(null);

            return result;
        };

        addColumn(columnName, Types.NUMERIC, precision, scale, false, wrapper);
    }

    protected void mapDecimal(String columnName, int precision, int scale, Func2<TEntity, BigDecimal> propertyGetter)
    {
        // We need to scale the incoming decimal, before writing it to SQL Server:
        final Func2<TEntity, BigDecimal> wrapper = entity -> {

            BigDecimal result = Optional.ofNullable(propertyGetter
                    .invoke(entity))
                    .map(d -> d.setScale(scale, BigDecimal.ROUND_HALF_UP))
                    .orElse(null);

            return result;
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
            Optional<BigInteger> resultAsBigIntegerOpt = Optional.ofNullable(propertyGetter.invoke(entity));

            return resultAsBigIntegerOpt.map(BigInteger::longValueExact).orElse(null);
        };

        addColumn(columnName, Types.BIGINT, wrapper);
    }

    protected void mapBigIntLong(String columnName, Func2<TEntity, Long> propertyGetter, boolean isAutoIncrement) {
        addColumn(columnName, Types.BIGINT, isAutoIncrement, propertyGetter);
    }

    protected void mapDate(String columnName, Func2<TEntity, LocalDate> propertyGetter) {
        addColumn(columnName, Types.DATE, propertyGetter);
    }

    protected void mapInstant(String columnName, Func2<TEntity, Instant> propertyGetter) {
        // We need to scale the incoming Instant and cast it to Timestamp so that the scaling sticks, before writing it to SQL Server:
        final Func2<TEntity, Timestamp> wrapper = entity -> {

            Instant result = propertyGetter.invoke(entity);
            if (result == null) {
                return null;
            }
            Timestamp castedResult = new Timestamp(result.toEpochMilli());
            castedResult.setNanos((result.getNano()/100)*100); // round to 100 nanoseconds (precision that SQL server can handle)
            return castedResult;
        };
        addColumn(columnName, Types.TIMESTAMP, wrapper);
    }

    protected void mapDateTime(String columnName, Func2<TEntity, Timestamp> propertyGetter) {
        addColumn(columnName, Types.TIMESTAMP, propertyGetter);
    }

    protected void mapLocalDateTime(String columnName, Func2<TEntity, LocalDateTime> propertyGetter) {
        // We need to scale the incoming LocalDateTime and cast it to Timestamp so that the scaling sticks, before writing it to SQL Server:
        final Func2<TEntity, Timestamp> wrapper = entity -> {

            LocalDateTime result = propertyGetter.invoke(entity);
            if (result == null) {
                return null;
            }
            long epochSeconds = result.toEpochSecond(OffsetDateTime.now().getOffset());
            Timestamp castedResult = new Timestamp(epochSeconds * 1000); // convert to milliseconds to create the Timestamp
            castedResult.setNanos((result.getNano()/100)*100); // round to 100 nanoseconds (precision that SQL server can handle)
            return castedResult;
        };
        addColumn(columnName, Types.TIMESTAMP, wrapper);
    }

    protected void mapUTCNano(String columnName, Func2<TEntity, Long> propertyGetter) {
        // We need to scale the incoming LocalDateTime and cast it to Timestamp so that the scaling sticks, before writing it to SQL Server:
        final Func2<TEntity, Timestamp> wrapper = entity -> {
            Long result = propertyGetter.invoke(entity);
            if (result == null) {
                return null;
            }
            long seconds = result / 1000000000; // loses subsecond data
            int nanoseconds = (int)(result - (seconds * 1000000000));
            nanoseconds = (nanoseconds/100)*100; // round to 100 nanoseconds (precision that SQL server can handle)
            LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(seconds,
                    nanoseconds,
                    ZoneOffset.UTC);
            long epochSeconds = localDateTime.toEpochSecond(OffsetDateTime.now().getOffset());
            Timestamp castedResult = new Timestamp(epochSeconds * 1000); // convert to milliseconds to create the Timestamp
            castedResult.setNanos(nanoseconds);
            return castedResult;
        };
        addColumn(columnName, Types.TIMESTAMP, wrapper);
    }

    protected void mapDouble(String columnName, Func2<TEntity, Double> propertyGetter)
    {
        addColumn(columnName, Types.DOUBLE, propertyGetter);
    }

    protected void mapInt(String columnName, Func2<TEntity, Integer> propertyGetter, boolean isAutoIncrement)
    {
        addColumn(columnName, Types.INTEGER, isAutoIncrement, propertyGetter);
    }

    protected void mapSmallInt(String columnName, Func2<TEntity, Short> propertyGetter, boolean isAutoIncrement)
    {
        addColumn(columnName, Types.SMALLINT, isAutoIncrement, propertyGetter);
    }

    protected void mapTinyInt(String columnName, Func2<TEntity, Byte> propertyGetter)
    {
        addColumn(columnName, Types.TINYINT, propertyGetter);
    }

    protected void mapTimeWithTimeZone(String columnName, Func2<TEntity, OffsetTime> propertyGetter) {
        addColumn(columnName, 2013, propertyGetter);
    }

    protected void mapDateTimeWithTimeZone(String columnName, Func2<TEntity, OffsetDateTime> propertyGetter) {
        addColumn(columnName, 2014, propertyGetter);
    }

    protected void mapString(String columnName, Func2<TEntity, String> propertyGetter) {
        addColumn(columnName, Types.NVARCHAR, propertyGetter);
    }

    protected void mapVarBinary(String columnName, int maxLength, Func2<TEntity, byte[]> propertyGetter) {
        addColumn(columnName, Types.VARBINARY, maxLength, 0, false, propertyGetter);
    }

    private <TProperty> void addColumn(String name, int type, boolean isAutoIncrement, Func2<TEntity, TProperty> propertyGetter)
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

    public TableDefinition getTableDefinition() {
        return table;
    }

    public List<ColumnDefinition<TEntity>> getColumns() {
        return columns;
    }
}