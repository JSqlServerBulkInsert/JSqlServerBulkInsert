package de.bytefish.jsqlserverbulkinsert.util;

import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.model.ColumnDefinition;
import de.bytefish.jsqlserverbulkinsert.model.IColumnDefinition;
import de.bytefish.jsqlserverbulkinsert.model.InformationSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InformationSchemaUtils {

    private static String SQL_COLUMN_INFORMATION_QUERY = "select COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION ASC";

    public static InformationSchema getInformationSchema(Connection connection, String schemaName, String tableName) throws Exception {

        // Queries the Information Schema to get the actual Column Ordinals:
        try(PreparedStatement ps = connection.prepareStatement(SQL_COLUMN_INFORMATION_QUERY)) {

            ps.setString(1, schemaName);
            ps.setString(2, tableName);

            // And now transform them:
            List<InformationSchema.ColumnInformation> results = new ArrayList<>();

            // Execute to the the ResultSet:
            try (ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    InformationSchema.ColumnInformation columnInformation = new InformationSchema.ColumnInformation(
                            rs.getString(1),
                            rs.getInt(2)
                    );

                    results.add(columnInformation);
                }

                return new InformationSchema(results);
            }
        }
    }

    private static <TEntity> InformationSchema internalGetInformationSchema(Connection connection, AbstractMapping<TEntity> mapping) {
        try {
            return getInformationSchema(connection, mapping.getTableDefinition().getSchema(), mapping.getTableDefinition().getTableName());
        } catch (Exception e) {
            // TODO Log Exception ...
            return null;
        }
    }

    public static <TEntity> void validateColumnMapping(Connection connection, AbstractMapping<TEntity> mapping) {

        // Try to obtain the Schema:
        InformationSchema informationSchema = internalGetInformationSchema(connection, mapping);

        // We cannot validate, perhaps no permissions to read the Information Schema, we shouldn't
        // stop at all, because this might be valid. Nevertheless it may lead to subtle errors and
        // we should probably log it in the future:
        if(informationSchema == null || informationSchema.getColumns() == null || informationSchema.getColumns().isEmpty()) {
            return;
        }

        // We cannot continue, if not all columns have been populated:
        if(mapping.getColumns().size() != informationSchema.getColumns().size()) {
            throw new RuntimeException("Destination Table has '" + informationSchema.getColumns().size() + "' columns, the Source Mapping has '" + mapping.getColumns().size() +"' columns.");
        }
    }

    public static <TEntity> List<IColumnDefinition<TEntity>> getSortedColumnMappings(Connection connection, AbstractMapping<TEntity> mapping) {

        // Try to get the Information Schema:
        InformationSchema informationSchema = internalGetInformationSchema(connection, mapping);

        // We cannot sort the mapping, perhaps no permissions to read the Information Schema, we should just
        // return the original mappings, because this might be intended. Nevertheless it may lead to subtle
        // errors and we should probably log it as a warning in the future:
        if(informationSchema == null || informationSchema.getColumns() == null || informationSchema.getColumns().isEmpty()) {
            return mapping.getColumns();
        }

        Map<String, IColumnDefinition<TEntity>> columnDefinitionMap = mapping.getColumns()
                .stream()
                .collect(Collectors.toMap(x -> x.getColumnMetaData().getName(), x -> x));

        List<IColumnDefinition<TEntity>> sortedColumns = new ArrayList<>();

        for (InformationSchema.ColumnInformation informationSchemaColumn : informationSchema.getColumns()) {
            IColumnDefinition<TEntity> columnDefinition = columnDefinitionMap.get(informationSchemaColumn.getColumnName());

            sortedColumns.add(columnDefinition);
        }

        return sortedColumns;
    }
}
