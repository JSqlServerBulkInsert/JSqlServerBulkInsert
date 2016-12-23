// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class DoubleMappingTest extends TransactionalTestBase {

    private class DoubleEntity extends SampleEntity<Double> {

        public DoubleEntity(Double value) {
            super(value);
        }
    }

    private class BigDecimalInsert extends SqlServerBulkInsert<DoubleEntity> {

        public BigDecimalInsert() {
            super("dbo", "UnitTest");

            mapDouble("DoubleValue", DoubleEntity::getValue);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        Double doubleValue = 123.123d;
        // Create the Value:
        List<DoubleEntity> entities = Arrays.asList(new DoubleEntity(doubleValue));
        // Create the BulkInserter:
        BigDecimalInsert localDateInsert = new BigDecimalInsert();
        // Now save all entities of a given stream:
        localDateInsert.saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        Double resultDoubleValue = rs.getDouble("DoubleValue");
        // Assert both are equal:
        Assert.assertEquals(doubleValue, resultDoubleValue, 1e-10);
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
                "                DoubleValue FLOAT\n" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }

}
