// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.records.SqlServerRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Stream;

public class SqlServerBulkInsert<TEntity> implements ISqlServerBulkInsert<TEntity> {

    private final AbstractMapping<TEntity> mapping;

    public SqlServerBulkInsert(AbstractMapping<TEntity> mapping)
    {
        this.mapping = mapping;
    }

    public void saveAll(Connection connection, Stream<TEntity> entities) {
        saveAll(connection, new SQLServerBulkCopyOptions(), entities);
    }

    public void saveAll(Connection connection, SQLServerBulkCopyOptions options, Stream<TEntity> entities) {
        // Create a new SQLServerBulkCopy Instance on the given Connection:
        try (SQLServerBulkCopy sqlServerBulkCopy = new SQLServerBulkCopy(connection)) {
            // Set the Options:
            sqlServerBulkCopy.setBulkCopyOptions(options);
            // The Destination Table to write to:
            sqlServerBulkCopy.setDestinationTableName(mapping.getTableDefinition().GetFullQualifiedTableName());
            // The SQL Records to insert:
            ISQLServerBulkRecord record = new SqlServerRecord<TEntity>(mapping.getColumns(), entities.iterator());
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
}
