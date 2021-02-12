package de.bytefish.jsqlserverbulkinsert.util;

import de.bytefish.jsqlserverbulkinsert.model.ColumnMetaData;
import de.bytefish.jsqlserverbulkinsert.model.InformationSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class SchemaUtils {

    public static List<InformationSchema.ColumnInformation> getColumnInformations(Connection connection, String schemaName, String tableName) throws Exception {

        // Queries the Information Schema to get the actual Column Ordinals:
        PreparedStatement ps = connection.prepareStatement("select COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?");

        ps.setString(1, schemaName);
        ps.setString(2, tableName);

        // Execute to the the ResultSet:
        ResultSet rs = ps.executeQuery();

        // And now transform them:
        List<InformationSchema.ColumnInformation> results = new ArrayList<>();

        while (rs.next()) {
            InformationSchema.ColumnInformation columnInformation = new InformationSchema.ColumnInformation(
                    rs.getString(1),
                    rs.getInt(2)
            );

            results.add(columnInformation);
        }

        return results;
    }
}
