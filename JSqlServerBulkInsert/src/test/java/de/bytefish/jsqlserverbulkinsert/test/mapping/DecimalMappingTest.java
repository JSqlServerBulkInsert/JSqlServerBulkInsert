// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

public class DecimalMappingTest extends TransactionalTestBase {

    private class BigDecimalEntity {

        private final BigDecimal value;

        public BigDecimalEntity(BigDecimal value) {
            this.value = value;
        }

        public BigDecimal getValue() {
            return value;
        }
    }

    private class BigDecimalEntityMapping extends AbstractMapping<BigDecimalEntity> {

        public BigDecimalEntityMapping() {
            super("dbo", "UnitTest");

            mapDecimal("DecimalValue", 20, 10, BigDecimalEntity::getValue);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkDecimalDataTest() throws SQLException {
        // Expected decimal 100.123
        BigDecimal bigDecimal = new BigDecimal("100.123");
        // Create entities
        List<BigDecimalEntity> entities = Arrays.asList(new BigDecimalEntity(bigDecimal));
        // Create the BulkInserter:
        BigDecimalEntityMapping mapping = new BigDecimalEntityMapping();
        // Now save all entities of a given stream:
        new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        BigDecimal resultBigDecimal = rs.getBigDecimal("DecimalValue");
        // Assert both are equal:
        Assert.assertEquals(bigDecimal.stripTrailingZeros(), resultBigDecimal.stripTrailingZeros());
        // Assert only one record was read:
        Assert.assertEquals(false, rs.next());
    }

    @Test
    public void bulkNullDataTest() throws SQLException {
        // Expected decimal: null
        BigDecimal bigDecimal = null;
        // Create entities
        List<BigDecimalEntity> entities = Arrays.asList(new BigDecimalEntity(bigDecimal));
        // Create the BulkInserter:
        BigDecimalEntityMapping mapping = new BigDecimalEntityMapping();
        // Now save all entities of a given stream:
        new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();
        // We have a Value:
        Assert.assertEquals(true, rs.next());
        // Get the Date we have written:
        BigDecimal resultBigDecimal = rs.getBigDecimal("DecimalValue");
        // Assert both are equal:
        Assert.assertEquals(null, resultBigDecimal);
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
                "                DecimalValue NUMERIC(20, 10)\n" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }

}
