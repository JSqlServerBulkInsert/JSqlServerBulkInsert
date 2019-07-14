// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import de.bytefish.jsqlserverbulkinsert.test.integration.IntegrationTest;
import de.bytefish.jsqlserverbulkinsert.test.model.Person;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

public class LocalDateMappingTest extends TransactionalTestBase {

    private class LocalDateEntity {

        private LocalDate localDate;

        private LocalDateEntity(LocalDate localDate) {
            this.localDate = localDate;
        }

        public LocalDate getLocalDate() {
            return localDate;
        }
    }

    private class LocalDateEntityMapping extends AbstractMapping<LocalDateEntity> {

        public LocalDateEntityMapping() {
            super("dbo", "UnitTest");

            mapDate("LocalDate", LocalDateEntity::getLocalDate);
        }

    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        // Expected LocalDate 2013/01/01:
        LocalDate localDate = LocalDate.of(2013, 1, 1);
        // Create te
        List<LocalDateEntity> persons = Arrays.asList(new LocalDateEntity(localDate));
        // Create the BulkInserter:
        LocalDateEntityMapping mapping = new LocalDateEntityMapping();
        // Now save all entities of a given stream:
        new SqlServerBulkInsert<>(mapping).saveAll(connection, persons.stream());
        // And assert all have been written to the database:
        ResultSet rs = getAll();

        while (rs.next()) {

            // Get the Date we have written:
            Date date = rs.getDate("LocalDate");

            // We should have a date:
            Assert.assertNotNull(date);

            // Get the LocalDate:
            LocalDate resultDate = date.toLocalDate();

            Assert.assertEquals(localDate.getYear(), resultDate.getYear());
            Assert.assertEquals(localDate.getMonthValue(), resultDate.getMonthValue());
            Assert.assertEquals(localDate.getDayOfMonth(), resultDate.getDayOfMonth());
        }
    }

    private ResultSet getAll() throws SQLException {

        String sqlStatement = "SELECT * FROM dbo.UnitTest";

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

    private void createTestTable() throws SQLException {
        String sqlStatement = "CREATE TABLE [dbo].[UnitTest]\n" +
                "            (\n" +
                "                LocalDate DATE\n" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }

}
