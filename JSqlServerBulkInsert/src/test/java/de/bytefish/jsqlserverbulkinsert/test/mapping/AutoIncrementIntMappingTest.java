// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

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

public class AutoIncrementIntMappingTest extends TransactionalTestBase {

    private class IntegerEntity extends SampleEntity<Integer> {

        public IntegerEntity(Integer value) {
            super(value);
        }
    }

    private class IntegerEntityMapping extends AbstractMapping<IntegerEntity> {

        public IntegerEntityMapping() {
            super("dbo", "UnitTest");

            mapInteger("PK_ID", x -> null, false);
            mapInteger("IntegerValue", IntegerEntity::getValue, false);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        Integer IntegerValue = 12;
        // Create the Value:
        List<IntegerEntity> entities = Arrays.asList(new IntegerEntity(IntegerValue), new IntegerEntity(IntegerValue));
        // Create the BulkInserter:
        IntegerEntityMapping mapping = new IntegerEntityMapping();
        // Create the Bulk Inserter:
        SqlServerBulkInsert<IntegerEntity> bulkInsert = new SqlServerBulkInsert<>(mapping);
        // Now save all entities of a given stream:
        bulkInsert.saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        Integer resultIntegerValue = rs.getInt("PK_ID");
        // Assert both are equal:
        Assert.assertEquals(new Integer(1), resultIntegerValue);

        // We have a second Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        Integer resultSecondIntegerValue = rs.getInt("PK_ID");
        // Assert both are equal:
        Assert.assertEquals(new Integer(2), resultSecondIntegerValue);

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
                "                PK_ID INT IDENTITY(1,1) PRIMARY KEY,\n" +
                "                IntegerValue INT" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }

}
