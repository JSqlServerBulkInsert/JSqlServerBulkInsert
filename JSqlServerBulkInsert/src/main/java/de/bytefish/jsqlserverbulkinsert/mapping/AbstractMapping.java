// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.mapping;

import de.bytefish.jsqlserverbulkinsert.converters.*;
import de.bytefish.jsqlserverbulkinsert.functional.ToBooleanFunction;
import de.bytefish.jsqlserverbulkinsert.functional.ToFloatFunction;
import de.bytefish.jsqlserverbulkinsert.model.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.*;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public abstract class AbstractMapping<TEntity> {

    private TableDefinition table;

    private List<IColumnDefinition<TEntity>> columns;

    public AbstractMapping(String tableName)
    {
        this("", tableName);
    }

    public AbstractMapping(String schemaName, String tableName)
    {
        this.table = new TableDefinition(schemaName, tableName);
        this.columns = new ArrayList<>();
    }

    protected void mapBoolean(String columnName, Function<TEntity, Boolean> propertyGetter) {
        mapProperty(columnName, Types.BIT, propertyGetter, new BooleanConverter());
    }

    protected void mapBoolean(String columnName, ToBooleanFunction<TEntity> propertyGetter) {
        mapProperty(columnName, Types.BIT, (entity) -> propertyGetter.applyAsBoolean(entity), new BooleanConverter());
    }

    // region Text Functions

    protected void mapChar(String columnName, Function<TEntity, Character> propertyGetter) {
        mapProperty(columnName, Types.CHAR, propertyGetter, new CharacterConverter());
    }

    protected void mapVarchar(String columnName, Function<TEntity, String> propertyGetter) {
        mapProperty(columnName, Types.VARCHAR, propertyGetter, new VarcharConverter());
    }

    protected void mapNvarchar(String columnName, Function<TEntity, String> propertyGetter) {
        mapProperty(columnName, Types.NVARCHAR, propertyGetter, new NVarcharConverter());
    }

    // endregion

    // region Numeric Functions

    protected void mapTinyInt(String columnName, Function<TEntity, Byte> propertyGetter)
    {
        mapProperty(columnName, Types.TINYINT, propertyGetter, new TinyIntConverter());
    }

    protected void mapSmallInt(String columnName, Function<TEntity, Short> propertyGetter)
    {
        mapProperty(columnName, Types.SMALLINT, propertyGetter, new SmallIntConverter());
    }

    protected void mapSmallInt(String columnName, Function<TEntity, Short> propertyGetter, boolean isAutoIncrement)
    {
        mapProperty(columnName, Types.SMALLINT, 0, 0, isAutoIncrement, propertyGetter, new SmallIntConverter());
    }

    protected void mapInteger(String columnName, Function<TEntity, Integer> propertyGetter)
    {
        mapProperty(columnName, Types.INTEGER, propertyGetter, new IntegerConverter());
    }

    protected void mapInteger(String columnName, Function<TEntity, Integer> propertyGetter, boolean isAutoIncrement)
    {
        mapProperty(columnName, Types.INTEGER, 0, 0, isAutoIncrement, propertyGetter, new IntegerConverter());
    }

    protected void mapInteger(String columnName, ToIntFunction<TEntity> propertyGetter)
    {
        mapProperty(columnName, Types.INTEGER, (entity) -> propertyGetter.applyAsInt(entity), new IntegerConverter());
    }

    protected void mapInteger(String columnName, ToIntFunction<TEntity> propertyGetter, boolean isAutoIncrement) {
        mapProperty(columnName, Types.INTEGER, (entity) -> propertyGetter.applyAsInt(entity), new IntegerConverter());
    }

    protected void mapLong(String columnName, Function<TEntity, Long> propertyGetter) {
        mapProperty(columnName, Types.BIGINT, propertyGetter, new LongConverter());
    }

    protected void mapLong(String columnName, Function<TEntity, Long> propertyGetter, boolean isAutoIncrement) {
        mapProperty(columnName, Types.BIGINT, 0, 0, isAutoIncrement, propertyGetter, new LongConverter());
    }

    protected void mapLong(String columnName, ToLongFunction<TEntity> propertyGetter) {
        mapProperty(columnName, Types.BIGINT,  (entity) -> propertyGetter.applyAsLong(entity), new LongConverter());
    }

    protected void mapLong(String columnName, ToLongFunction<TEntity> propertyGetter, boolean isAutoIncrement) {
        mapProperty(columnName, Types.BIGINT,  0, 0, isAutoIncrement, (entity) -> propertyGetter.applyAsLong(entity), new LongConverter());
    }

    protected void mapNumeric(String columnName, int precision, int scale, Function<TEntity, BigDecimal> propertyGetter) {
        mapProperty(columnName, Types.NUMERIC, precision, scale, false, propertyGetter, new BigDecimalConverter(scale));
    }

    protected void mapDecimal(String columnName, int precision, int scale, Function<TEntity, BigDecimal> propertyGetter) {
        mapProperty(columnName, Types.DECIMAL, precision, scale, false, propertyGetter, new BigDecimalConverter(scale));
    }

    protected void mapReal(String columnName, Function<TEntity, Float> propertyGetter) {
        mapProperty(columnName, Types.REAL, propertyGetter, new FloatConverter());
    }

    protected void mapReal(String columnName, ToFloatFunction<TEntity> propertyGetter) {
        mapProperty(columnName, Types.REAL, (entity) -> propertyGetter.applyAsFloat(entity), new FloatConverter());
    }

    protected void mapBigInt(String columnName, Function<TEntity, BigInteger> propertyGetter) {
        mapProperty(columnName, Types.BIGINT, propertyGetter, new BigIntegerConverter());
    }

    protected void mapDouble(String columnName, Function<TEntity, Double> propertyGetter) {
        mapProperty(columnName, Types.DOUBLE, propertyGetter, new DoubleConverter());
    }

    protected void mapDouble(String columnName, ToDoubleFunction<TEntity> propertyGetter) {
        mapProperty(columnName, Types.DOUBLE, (entity) -> propertyGetter.applyAsDouble(entity), new DoubleConverter());
    }

    // endregion

    // region Time Functions

    protected void mapDate(String columnName, Function<TEntity, LocalDate> propertyGetter) {
        mapProperty(columnName, Types.DATE, propertyGetter, new LocalDateConverter());
    }

    protected void mapInstant(String columnName, Function<TEntity, Instant> propertyGetter) {

        mapProperty(columnName, Types.TIMESTAMP, propertyGetter, new InstantConverter());
    }

    protected void mapDateTime(String columnName, Function<TEntity, Timestamp> propertyGetter) {
        mapProperty(columnName, Types.TIMESTAMP, propertyGetter, new TimestampConverter());
    }

    protected void mapLocalDateTime(String columnName, Function<TEntity, LocalDateTime> propertyGetter) {
        mapProperty(columnName, Types.TIMESTAMP, propertyGetter, new LocalDateTimeConverter());
    }

    protected void mapTimeWithTimeZone(String columnName, Function<TEntity, OffsetTime> propertyGetter) {
        mapProperty(columnName, SqlServerTypes.TimeWithTimeZone, propertyGetter, new OffsetTimeConverter());
    }

    protected void mapDateTimeWithTimeZone(String columnName, Function<TEntity, OffsetDateTime> propertyGetter) {
        mapProperty(columnName, SqlServerTypes.DateTimeWithTimeZone, propertyGetter, new OffsetDateTimeConverter());
    }

//
//    protected void mapUTCNano(String dateColumnName, String timeColumnName, Function<TEntity, Long> propertyGetter) {
//
//        // We need to scale the incoming LocalDateTime and cast it to Timestamp so that the scaling sticks, before writing it to SQL Server:
//        final Function<TEntity, Date> dateWrapper = entity -> {
//            Long result = propertyGetter.apply(entity);
//
//            if (result == null) {
//                return null;
//            }
//
//            AbstractMap.Entry<Long,Integer> convertedDT = convertUTCNanoToEpochSecAndNano(result);
//
//            return new Date(convertedDT.getKey()*1000);
//        };
//
//        final Function<TEntity, String> timeWrapper = entity -> {
//            Long result = propertyGetter.apply(entity);
//
//            if (result == null) {
//                return null;
//            }
//
//            AbstractMap.Entry<Long,Integer> convertedDT = convertUTCNanoToEpochSecAndNano(result);
//            Time t = new Time(convertedDT.getKey() * 1000);
//            return t.toString() + "." + convertedDT.getValue()/100; // send as string bc java.sql.Time supports only millisecond precision!?
//        };
//
//        addColumn(dateColumnName, Types.DATE, dateWrapper);
//        addColumn(timeColumnName, Types.TIME, timeWrapper);
//    }
//
//    protected void mapUTCNano(String columnName, Function<TEntity, Long> propertyGetter) {
//
//        // We need to scale the incoming LocalDateTime and cast it to Timestamp so that the scaling sticks, before writing it to SQL Server:
//        final Function<TEntity, Timestamp> wrapper = entity -> {
//
//            Long result = propertyGetter.apply(entity);
//
//            if (result == null) {
//                return null;
//            }
//
//            AbstractMap.Entry<Long,Integer> convertedDT = convertUTCNanoToEpochSecAndNano(result);
//
//            Timestamp castedResult = new Timestamp(convertedDT.getKey() * 1000); // convert to milliseconds to create the Timestamp
//            castedResult.setNanos(convertedDT.getValue());
//
//            return castedResult;
//        };
//
//        addColumn(columnName, Types.TIMESTAMP, wrapper);
//    }


    // endregion

    // region Binary

    protected void mapVarBinary(String columnName, int maxLength, Function<TEntity, byte[]> propertyGetter) {
        mapProperty(columnName, Types.VARBINARY, maxLength, 0, false, propertyGetter, new VarbinaryConverter());
    }

    // endregion

    public <TProperty> void mapProperty(String name, int type, Function<TEntity, TProperty> propertyGetter, IConverter<TProperty> converter)
    {
        // Create the current Column Meta Data:
        ColumnMetaData columnMetaData = new ColumnMetaData(name, type);

        // Add a new Column with the Meta Data and Property Getter:
        addColumn(columnMetaData, propertyGetter, converter);
    }

    public <TProperty> void mapProperty(String name, int type, int precision, int scale, boolean isAutoIncrement, Function<TEntity, TProperty> propertyGetter, IConverter<TProperty> converter)
    {
        // Create the current Column Meta Data:
        ColumnMetaData columnMetaData = new ColumnMetaData(name, type, precision, scale, isAutoIncrement);

        // Add a new Column with the Meta Data and Property Getter:
        addColumn(columnMetaData, propertyGetter, converter);
    }

    public <TProperty> void addColumn(ColumnMetaData columnMetaData, Function<TEntity, TProperty> propertyGetter, IConverter<TProperty> converter)
    {
        // Add a new Column with the Meta Data and Property Getter:
        ColumnDefinition<TEntity, TProperty> columnDefinition = new ColumnDefinition<>(columnMetaData, propertyGetter, converter);

        columns.add(columnDefinition);
    }

    public TableDefinition getTableDefinition() {
        return table;
    }

    public List<IColumnDefinition<TEntity>> getColumns() {
        return columns;
    }
}