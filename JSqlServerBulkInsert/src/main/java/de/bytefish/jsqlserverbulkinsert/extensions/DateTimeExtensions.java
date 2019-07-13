// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.extensions;

import de.bytefish.jsqlserverbulkinsert.converters.BaseConverter;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.util.TimestampUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.AbstractMap;
import java.util.function.Function;

public class DateTimeExtensions {

    public static <TEntity> void mapUTCNano(AbstractMapping<TEntity> mapping, String dateColumnName, String timeColumnName, Function<TEntity, Long> propertyGetter) {

        mapping.mapProperty(dateColumnName, Types.DATE, propertyGetter, new BaseConverter<Long>() {
            @Override
            public Object internalConvert(Long value) {
                AbstractMap.SimpleImmutableEntry<Long, Integer> convertedDT = TimestampUtils.convertUtcNanoToEpochSecAndNano(value);

                return new Date(convertedDT.getKey() * 1000);
            }
        });

        mapping.mapProperty(timeColumnName, Types.TIME, propertyGetter, new BaseConverter<Long>() {
            @Override
            public Object internalConvert(Long value) {
                AbstractMap.SimpleImmutableEntry<Long, Integer> convertedDT = TimestampUtils.convertUtcNanoToEpochSecAndNano(value);
                Time t = new Time(convertedDT.getKey() * 1000);
                return t.toString() + "." + convertedDT.getValue() / 100; // send as string bc java.sql.Time supports only millisecond precision!?

            }
        });
    }

    public static <TEntity> void mapUTCNano(AbstractMapping<TEntity> mapping, String columnName, Function<TEntity, Long> propertyGetter) {

        mapping.mapProperty(columnName, Types.TIMESTAMP, propertyGetter, new BaseConverter<Long>() {
            @Override
            public Object internalConvert(Long value) {

                AbstractMap.SimpleImmutableEntry<Long, Integer> convertedDT = TimestampUtils.convertUtcNanoToEpochSecAndNano(value);

                Timestamp castedResult = new Timestamp(convertedDT.getKey() * 1000);
                castedResult.setNanos(convertedDT.getValue());

                return castedResult;
            }
        });
    }


}
