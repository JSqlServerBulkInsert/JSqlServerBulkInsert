// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert;

import com.microsoft.sqlserver.jdbc.*;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.model.IColumnDefinition;
import de.bytefish.jsqlserverbulkinsert.records.SqlServerBulkData;
import de.bytefish.jsqlserverbulkinsert.util.SchemaUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class SqlServerBulkInsert<TEntity> implements ISqlServerBulkInsert<TEntity> {

    private List<IColumnDefinition<TEntity>> columnMappings;

    private final AbstractMapping<TEntity> mapping;

    public SqlServerBulkInsert(AbstractMapping<TEntity> mapping)
    {
        this.mapping = mapping;
    }

    public void saveAll(Connection connection, Stream<TEntity> entities) {
        saveAll(connection, new SQLServerBulkCopyOptions(), entities);
    }

    public void saveAll(Connection connection, SQLServerBulkCopyOptions options, Stream<TEntity> entities) {

        validateAndInitializeWithMetaData(connection);

        // Create a new SQLServerBulkCopy Instance on the given Connection:
        try (SQLServerBulkCopy sqlServerBulkCopy = new SQLServerBulkCopy(connection)) {
            // Set the Options:
            sqlServerBulkCopy.setBulkCopyOptions(options);
            // The Destination Table to write to:
            sqlServerBulkCopy.setDestinationTableName(mapping.getTableDefinition().GetFullQualifiedTableName());
            // Resort
            List<IColumnDefinition<TEntity>> columnMappings = this.columnMappings;
            // The SQL Records to insert:
            ISQLServerBulkData record = new SqlServerBulkData<TEntity>(columnMappings, entities.iterator());
            // Finally start the Bulk Copy Process:
            sqlServerBulkCopy.writeToServer(record);
            // Handle Exceptions:
        } catch (SQLServerException e) {
            // Wrap it in a RunTimeException to provide a nice API:
            throw new RuntimeException(e);
        }
    }

    public void saveAll(Connection connection, Collection<TEntity> entities) throws SQLException {
        if(entities == null) {
            throw new IllegalArgumentException("entities");
        }

        saveAll(connection, entities.stream());
    }

    private synchronized void validateAndInitializeWithMetaData(Connection connection) {
        if(columnMappings == null) {
            // Make sure the mapping is valid:
            SchemaUtils.validateColumnMapping(connection, mapping);

            // Now set this Bulk Inserters mappings:
            this.columnMappings = SchemaUtils.getSortedColumnMappings(connection, mapping);
        }
    }
}
