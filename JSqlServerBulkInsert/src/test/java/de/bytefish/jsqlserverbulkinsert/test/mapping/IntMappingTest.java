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

public class IntMappingTest extends TransactionalTestBase {

    private class IntegerEntity extends SampleEntity<Integer> {

        public IntegerEntity(Integer value) {
            super(value);
        }
    }

    private class IntegerEntityMapping extends AbstractMapping<IntegerEntity> {

        public IntegerEntityMapping() {
            super("dbo", "UnitTest");

            mapInteger("IntegerValue", IntegerEntity::getValue);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        Integer IntegerValue = 214483647;
        // Create the Value:
        List<IntegerEntity> entities = Arrays.asList(new IntegerEntity(IntegerValue));
        // Create the BulkInserter:
        IntegerEntityMapping mapping = new IntegerEntityMapping();
        // Now save all entities of a given stream:
        new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        Integer resultIntegerValue = rs.getInt("IntegerValue");
        // Assert both are equal:
        Assert.assertEquals(IntegerValue, resultIntegerValue);
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
                "                IntegerValue INT\n" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }

}
