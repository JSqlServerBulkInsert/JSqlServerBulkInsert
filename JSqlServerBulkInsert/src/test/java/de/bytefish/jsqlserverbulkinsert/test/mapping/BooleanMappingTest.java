// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

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

public class BooleanMappingTest extends TransactionalTestBase {

    private class BooleanEntity extends SampleEntity<Boolean> {

        public BooleanEntity(Boolean value) {
            super(value);
        }
    }

    private class BooleanEntityMapping extends AbstractMapping<BooleanEntity> {

        public BooleanEntityMapping() {
            super("dbo", "UnitTest");

            mapBoolean("BooleanValue", BooleanEntity::getValue);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        boolean booleanValue = true;
        // Create the Value:
        List<BooleanEntity> entities = Arrays.asList(new BooleanEntity(booleanValue));
        // Create the BulkInserter:
        BooleanEntityMapping mapping = new BooleanEntityMapping();
        // Now save all entities of a given stream:
        new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        boolean resultBooleanValue = rs.getBoolean("BooleanValue");
        // Assert both are equal:
        Assert.assertEquals(booleanValue, resultBooleanValue);
        // Assert only one record was read:
        Assert.assertEquals(false, rs.next());
    }

    private ResultSet getAll() throws SQLException {

        String sqlStatement = "SELECT * FROM dbo.UnitTest";

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

    private void createTestTable() throws SQLException {
        String sqlStatement = "CREATE TABLE [dbo].[UnitTest]\n" +
                "            (\n" +
                "                BooleanValue BIT\n" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }

}
