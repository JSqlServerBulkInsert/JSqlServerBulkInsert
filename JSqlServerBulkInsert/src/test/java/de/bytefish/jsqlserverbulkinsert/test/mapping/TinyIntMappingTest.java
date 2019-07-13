// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
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

public class TinyIntMappingTest extends TransactionalTestBase {

    private class ByteEntity extends SampleEntity<Byte> {

        public ByteEntity(Byte value) {
            super(value);
        }
    }

    private class ByteEntityMapping extends AbstractMapping<ByteEntity> {

        public ByteEntityMapping() {
            super("dbo", "UnitTest");

            mapTinyInt("ByteValue", ByteEntity::getValue);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        Byte ByteValue = 15;
        // Create the Value:
        List<ByteEntity> entities = Arrays.asList(new ByteEntity(ByteValue));
        // Create the BulkInserter:
        ByteEntityMapping mapping = new ByteEntityMapping();
        // Now save all entities of a given stream:
        new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        Byte resultByteValue = rs.getByte("ByteValue");
        // Assert both are equal:
        Assert.assertEquals(ByteValue, resultByteValue);
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
                "                ByteValue TINYINT\n" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }

}
