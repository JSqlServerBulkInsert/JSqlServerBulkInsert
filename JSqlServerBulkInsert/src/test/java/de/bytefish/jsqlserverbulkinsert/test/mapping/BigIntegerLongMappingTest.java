// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;

public class BigIntegerLongMappingTest extends TransactionalTestBase {

    private class BigIntegerEntity extends SampleEntity<Long> {

        public BigIntegerEntity(Long value) {
            super(value);
        }
    }

    private class BigIntegerMapping extends AbstractMapping<BigIntegerEntity> {

        public BigIntegerMapping() {
            super("dbo", "UnitTest");

            mapBigIntLong("BigIntegerValue", BigIntegerEntity::getValue);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        long longValue = 47878778228484l;
        // Create the Value:
        List<BigIntegerEntity> entities = Arrays.asList(new BigIntegerEntity(longValue));
        // Create the Mapping:
        BigIntegerMapping mapping = new BigIntegerMapping();
        // Create the Bulk Inserter:
        SqlServerBulkInsert<BigIntegerEntity> bulkInsert = new SqlServerBulkInsert<>(mapping);
        // Now save all entities of a given stream:
        bulkInsert.saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        long resultBigIntegerValue = rs.getLong("BigIntegerValue");
        // Assert both are equal:
        Assert.assertEquals(longValue, resultBigIntegerValue);
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
