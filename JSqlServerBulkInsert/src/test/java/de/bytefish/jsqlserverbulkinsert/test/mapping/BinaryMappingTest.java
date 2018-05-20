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

public class BinaryMappingTest extends TransactionalTestBase {

    private class BinaryDataEntity extends SampleEntity<byte[]> {

        public BinaryDataEntity(byte[] value) {
            super(value);
        }
    }

    private class ByteDataEntityMapping extends AbstractMapping<BinaryDataEntity> {

        public ByteDataEntityMapping() {
            super("dbo", "UnitTest");

            mapVarBinary("ByteValue", 255, BinaryDataEntity::getValue);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        byte[] binaryDataValue = new byte[] { 1, 2, 3};
        // Create the Value:
        List<BinaryDataEntity> entities = Arrays.asList(new BinaryDataEntity(binaryDataValue));
        // Create the BulkInserter:
        ByteDataEntityMapping mapping = new ByteDataEntityMapping();
        // Now save all entities of a given stream:
        new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        byte[] resultByteValue = rs.getBytes("ByteValue");
        // Assert both are equal:
        Assert.assertEquals(binaryDataValue.length, resultByteValue.length);
        // Check Content:
        for(int i = 0; i < resultByteValue.length; i++) {
            Assert.assertEquals((byte) binaryDataValue[i], (byte) resultByteValue[i]);
        }
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
                "                ByteValue VARBINARY(255)\n" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }

}
