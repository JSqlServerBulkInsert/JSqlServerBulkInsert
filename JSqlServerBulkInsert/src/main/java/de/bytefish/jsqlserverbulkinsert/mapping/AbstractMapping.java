// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.mapping;

import de.bytefish.jsqlserverbulkinsert.converters.*;
import de.bytefish.jsqlserverbulkinsert.functional.ToBooleanFunction;
import de.bytefish.jsqlserverbulkinsert.functional.ToFloatFunction;
import de.bytefish.jsqlserverbulkinsert.model.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
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
        mapProperty(columnName, Types.BIT, propertyGetter, new IdentityConverter<>());
    }

    protected void mapBoolean(String columnName, ToBooleanFunction<TEntity> propertyGetter) {
        mapProperty(columnName, Types.BIT, (entity) -> propertyGetter.applyAsBoolean(entity), new IdentityConverter());
    }

    // region Text Functions

    protected void mapChar(String columnName, Function<TEntity, Character> propertyGetter) {
        mapProperty(columnName, Types.CHAR, propertyGetter, new IdentityConverter());
    }

    protected void mapNchar(String columnName, Function<TEntity, Character> propertyGetter) {
        mapProperty(columnName, Types.NCHAR, propertyGetter, new IdentityConverter());
    }

    protected void mapClob(String columnName, Function<TEntity, Character> propertyGetter) {
        mapProperty(columnName, Types.CLOB, propertyGetter, new IdentityConverter());
    }

    protected void mapVarchar(String columnName, Function<TEntity, String> propertyGetter) {
        mapProperty(columnName, Types.VARCHAR, propertyGetter, new IdentityConverter());
    }

    protected void mapLongVarchar(String columnName, Function<TEntity, Character> propertyGetter) {
        mapProperty(columnName, Types.LONGVARCHAR, propertyGetter, new IdentityConverter());
    }

    protected void mapNvarchar(String columnName, Function<TEntity, String> propertyGetter) {
        mapProperty(columnName, Types.NVARCHAR, propertyGetter, new IdentityConverter());
    }

    protected void mapLongNvarchar(String columnName, Function<TEntity, Character> propertyGetter) {
        mapProperty(columnName, Types.LONGNVARCHAR, propertyGetter, new IdentityConverter());
    }


    // endregion

    // region Special Functions

    protected <TProperty>  void mapNull(String columnName, Function<TEntity, TProperty> propertyGetter) {
        mapProperty(columnName, Types.NULL, propertyGetter, new NullConverter<>());
    }

    // endregion

    // region Numeric Functions

    protected void mapTinyInt(String columnName, Function<TEntity, Byte> propertyGetter)
    {
        mapProperty(columnName, Types.TINYINT, propertyGetter, new IdentityConverter());
    }

    protected void mapSmallInt(String columnName, Function<TEntity, Short> propertyGetter)
    {
        mapProperty(columnName, Types.SMALLINT, propertyGetter, new IdentityConverter());
    }

    protected void mapSmallInt(String columnName, boolean isAutoIncrement)
    {
        mapProperty(columnName, Types.SMALLINT, 0, 0, isAutoIncrement, (entity) -> null, new IdentityConverter());
    }

    protected void mapInteger(String columnName, Function<TEntity, Integer> propertyGetter)
    {
        mapProperty(columnName, Types.INTEGER, propertyGetter, new IdentityConverter());
    }

    protected void mapInteger(String columnName, boolean isAutoIncrement)
    {
        mapProperty(columnName, Types.INTEGER, 0, 0, isAutoIncrement, (entity) -> null, new IdentityConverter());
    }

    protected void mapInteger(String columnName, ToIntFunction<TEntity> propertyGetter)
    {
        mapProperty(columnName, Types.INTEGER, (entity) -> propertyGetter.applyAsInt(entity), new IdentityConverter());
    }

    protected void mapLong(String columnName, Function<TEntity, Long> propertyGetter) {
        mapProperty(columnName, Types.BIGINT, propertyGetter, new IdentityConverter());
    }

    protected void mapLong(String columnName, boolean isAutoIncrement) {
        mapProperty(columnName, Types.BIGINT, 0, 0, isAutoIncrement, (entity) -> null, new IdentityConverter());
    }

    protected void mapLong(String columnName, ToLongFunction<TEntity> propertyGetter) {
        mapProperty(columnName, Types.BIGINT,  (entity) -> propertyGetter.applyAsLong(entity), new IdentityConverter());
    }

    protected void mapNumeric(String columnName, int precision, int scale, Function<TEntity, BigDecimal> propertyGetter) {
        mapProperty(columnName, Types.NUMERIC, precision, scale, false, propertyGetter, new BigDecimalConverter(scale, RoundingMode.HALF_UP));
    }
    protected void mapNumeric(String columnName, int precision, int scale, RoundingMode roundingMode, Function<TEntity, BigDecimal> propertyGetter) {
        mapProperty(columnName, Types.NUMERIC, precision, scale, false, propertyGetter, new BigDecimalConverter(scale, roundingMode));
    }

    protected void mapDecimal(String columnName, int precision, int scale, Function<TEntity, BigDecimal> propertyGetter) {
        mapProperty(columnName, Types.DECIMAL, precision, scale, false, propertyGetter, new BigDecimalConverter(scale, RoundingMode.HALF_UP));
    }

    protected void mapReal(String columnName, Function<TEntity, Float> propertyGetter) {
        mapProperty(columnName, Types.REAL, propertyGetter, new IdentityConverter());
    }

    protected void mapReal(String columnName, ToFloatFunction<TEntity> propertyGetter) {
        mapProperty(columnName, Types.REAL, (entity) -> propertyGetter.applyAsFloat(entity), new IdentityConverter());
    }

    protected void mapBigInt(String columnName, Function<TEntity, BigInteger> propertyGetter) {
        mapProperty(columnName, Types.BIGINT, propertyGetter, new BigIntegerConverter());
    }

    protected void mapDouble(String columnName, Function<TEntity, Double> propertyGetter) {
        mapProperty(columnName, Types.DOUBLE, propertyGetter, new IdentityConverter());
    }

    protected void mapDouble(String columnName, ToDoubleFunction<TEntity> propertyGetter) {
        mapProperty(columnName, Types.DOUBLE, (entity) -> propertyGetter.applyAsDouble(entity), new IdentityConverter());
    }

    // endregion

    // region Time Functions

    protected void mapDate(String columnName, Function<TEntity, LocalDate> propertyGetter) {
        mapProperty(columnName, Types.DATE, propertyGetter, new IdentityConverter());
    }

    protected void mapInstant(String columnName, Function<TEntity, Instant> propertyGetter) {

        mapProperty(columnName, Types.TIMESTAMP, propertyGetter, new InstantConverter());
    }

    protected void mapDateTime(String columnName, Function<TEntity, Timestamp> propertyGetter) {
        mapProperty(columnName, Types.TIMESTAMP, propertyGetter, new IdentityConverter());
    }

    protected void mapLocalDateTime(String columnName, Function<TEntity, LocalDateTime> propertyGetter) {
        mapProperty(columnName, Types.TIMESTAMP, propertyGetter, new LocalDateTimeConverter());
    }

    protected void mapTimeWithTimeZone(String columnName, Function<TEntity, OffsetTime> propertyGetter) {
        mapProperty(columnName, SqlServerTypes.TimeWithTimeZone, propertyGetter, new IdentityConverter());
    }

    protected void mapDateTimeWithTimeZone(String columnName, Function<TEntity, OffsetDateTime> propertyGetter) {
        mapProperty(columnName, SqlServerTypes.DateTimeWithTimeZone, propertyGetter, new IdentityConverter());
    }

    // endregion

    // region Binary

    protected void mapVarBinary(String columnName, int maxLength, Function<TEntity, byte[]> propertyGetter) {
        mapProperty(columnName, Types.VARBINARY, maxLength, 0, false, propertyGetter, new IdentityConverter());
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