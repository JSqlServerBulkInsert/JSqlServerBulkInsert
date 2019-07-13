package de.bytefish.jsqlserverbulkinsert.converters;

import java.sql.Types;

public abstract class BaseConverter<TSourceType> implements IConverter<TSourceType> {

    public Object convert(TSourceType value) {
        if(value == null) {
            return null;
        }

        return internalConvert(value);
    }

    public abstract Object internalConvert(TSourceType value);
}
