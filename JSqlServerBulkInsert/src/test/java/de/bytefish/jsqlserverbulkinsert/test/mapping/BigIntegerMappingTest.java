// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class BigIntegerMappingTest extends TransactionalTestBase {

    private class BigIntegerEntity extends SampleEntity<BigInteger> {

        public BigIntegerEntity(BigInteger value) {
            super(value);
        }
    }

    private class BigIntegerInsert extends SqlServerBulkInsert<BigIntegerEntity> {

        public BigIntegerInsert() {
            super("dbo", "UnitTest");

            mapBigInt("BigIntegerValue", BigIntegerEntity::getValue);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        BigInteger BigIntegerValue = new BigInteger("47878778228484");
        // Create the Value:
        List<BigIntegerEntity> entities = Arrays.asList(new BigIntegerEntity(BigIntegerValue));
        // Create the BulkInserter:
        BigIntegerInsert localDateInsert = new BigIntegerInsert();
        // Now save all entities of a given stream:
        localDateInsert.saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        long resultBigIntegerValue = rs.getLong("BigIntegerValue");
        // Assert both are equal:
        Assert.assertEquals(BigIntegerValue.longValueExact(), resultBigIntegerValue);
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
                "                BigIntegerValue bigint\n" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }

}
