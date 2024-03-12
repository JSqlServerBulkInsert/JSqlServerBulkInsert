package de.bytefish.jsqlserverbulkinsert.util;

import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.model.IColumnDefinition;
import de.bytefish.jsqlserverbulkinsert.model.SchemaMetaData;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

public class SchemaUtils {

    public static SchemaMetaData getSchemaMetaData(Connection connection, String schemaName, String tableName) throws Exception {

        DatabaseMetaData databaseMetaData = connection.getMetaData();

        // Query the DatabaseMetaData using the JDBC built-in functionality:
        ResultSet rs = databaseMetaData.getColumns(null, schemaName, tableName, null);

        // And now transform them:
        List<SchemaMetaData.ColumnInformation> columnInformations = new ArrayList<>();

        while (rs.next()) {
            if(rs.getString("IS_GENERATEDCOLUMN").equals("NO")) {
                SchemaMetaData.ColumnInformation columnInformation = new SchemaMetaData.ColumnInformation(
                        rs.getString("COLUMN_NAME"),
                        rs.getInt("ORDINAL_POSITION")
                );

                columnInformations.add(columnInformation);
            }
        }

        // Make sure they are sorted ascending by Ordinals:
        List<SchemaMetaData.ColumnInformation> sortedColumns = columnInformations.stream()
                .sorted(Comparator.comparing(x -> x.getOrdinal()))
                .collect(Collectors.toList());

        return new SchemaMetaData(sortedColumns);
    }

    private static <TEntity> SchemaMetaData internalGetSchemaMetaData(Connection connection, AbstractMapping<TEntity> mapping) {
        try {
            return getSchemaMetaData(connection, mapping.getTableDefinition().getSchema(), mapping.getTableDefinition().getTableName());
        } catch (Exception e) {
            // TODO Log Exception ...
            return null;
        }
    }

    public static <TEntity> void validateColumnMapping(Connection connection, AbstractMapping<TEntity> mapping) {

        // Try to obtain the Schema:
        SchemaMetaData schemaMetaData = internalGetSchemaMetaData(connection, mapping);

        // We cannot validate, perhaps no permissions to read the Meta Data? We shouldn't throw an
        // Exception, because this might be valid. Nevertheless it may lead to subtle errors and
        // we should probably log it in the future:
        if(schemaMetaData == null || schemaMetaData.getColumns() == null || schemaMetaData.getColumns().isEmpty()) {
            return;
        }

        // We have read the DatabaseMetaData and cannot continue, if not all columns have been populated:
        if(mapping.getColumns().size() != schemaMetaData.getColumns().size()) {
            throw new RuntimeException("Destination Table has '" + schemaMetaData.getColumns().size() + "' columns, the Source Mapping has '" + mapping.getColumns().size() +"' columns.");
        }
    }

    public static <TEntity> List<IColumnDefinition<TEntity>> getSortedColumnMappings(Connection connection, AbstractMapping<TEntity> mapping) {

        // Try to get the SchemaMetaData:
        SchemaMetaData schemaMetaData = internalGetSchemaMetaData(connection, mapping);

        // We cannot sort the mapping, perhaps no permissions to read the Meta Data, we should just
        // return the original mappings, because this might be intended. Nevertheless it may lead to subtle
        // errors and we should probably log it as a warning in the future:
        if(schemaMetaData == null || schemaMetaData.getColumns() == null || schemaMetaData.getColumns().isEmpty()) {
            return mapping.getColumns();
        }

        // Build a Lookup Table:
        Map<String, IColumnDefinition<TEntity>> columnDefinitionLookup = mapping.getColumns()
                .stream()
                .collect(Collectors.toMap(x -> x.getColumnMetaData().getName().toUpperCase(), x -> x));

        // Now Sort the Column Definitions:
        List<IColumnDefinition<TEntity>> sortedColumns = new ArrayList<>();

        for (SchemaMetaData.ColumnInformation columnMetaData : schemaMetaData.getColumns()) {
            IColumnDefinition<TEntity> columnDefinition = columnDefinitionLookup.get(columnMetaData.getColumnName().toUpperCase());

            sortedColumns.add(columnDefinition);
        }

        return sortedColumns;
    }
}
