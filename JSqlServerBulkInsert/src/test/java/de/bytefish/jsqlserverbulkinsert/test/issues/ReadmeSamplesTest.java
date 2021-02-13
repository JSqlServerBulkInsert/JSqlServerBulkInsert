// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.issues;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class ReadmeSamplesTest extends TransactionalTestBase {

    private class MySampleEntity {

        private final int val;

        public MySampleEntity(int val) {
            this.val = val;
        }

        public Integer getVal() {
            return val;
        }
    }

    private class MySampleEntityMapping extends AbstractMapping<MySampleEntity> {

        public MySampleEntityMapping() {
            super("dbo", "UnitTest");

            mapInteger("PK_ID",true);
            mapInteger("IntegerValue", x -> x.getVal());
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertAutoIncrementing() throws SQLException {
        // Create the Value:
        List<MySampleEntity> entities = Arrays.asList(new MySampleEntity(11), new MySampleEntity(12));
        // Create the BulkInserter:
        MySampleEntityMapping mapping = new MySampleEntityMapping();
        // Create the Bulk Inserter:
        SqlServerBulkInsert<MySampleEntity> bulkInsert = new SqlServerBulkInsert<>(mapping);
        // Now save all entities of a given stream:
        bulkInsert.saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        Integer resultIntegerValue = rs.getInt("PK_ID");

        // Now assert the Entities have been inserted with the PK 1 and 2:
        Assert.assertEquals(Integer.valueOf(1), resultIntegerValue);
        // We have a second Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        Integer resultSecondIntegerValue = rs.getInt("PK_ID");
        // Assert both are equal:
        Assert.assertEquals(Integer.valueOf(2), resultSecondIntegerValue);
    }

    private ResultSet getAll() throws SQLException {

        String sqlStatement = "SELECT * FROM dbo.UnitTest";

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

    private void createTestTable() throws SQLException {
        String sqlStatement = "CREATE TABLE [dbo].[UnitTest]\n" +
                "            (\n" +
                "                PK_ID INT IDENTITY(1,1) PRIMARY KEY,\n" +
                "                IntegerValue INT" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }
}
