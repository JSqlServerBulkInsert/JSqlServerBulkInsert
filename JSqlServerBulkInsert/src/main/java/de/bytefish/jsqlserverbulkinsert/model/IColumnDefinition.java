package de.bytefish.jsqlserverbulkinsert.model;

public interface IColumnDefinition<TEntityType> {

    Object getPropertyValue(TEntityType entity);

    ColumnMetaData getColumnMetaData();

}

