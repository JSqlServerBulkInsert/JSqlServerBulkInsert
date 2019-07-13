package de.bytefish.jsqlserverbulkinsert.converters;

public interface IConverter<TSourceType> {

    Object convert(TSourceType value);
}
