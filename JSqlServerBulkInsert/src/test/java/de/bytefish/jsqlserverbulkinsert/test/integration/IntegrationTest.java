// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.integration;

import de.bytefish.jsqlserverbulkinsert.test.model.Person;
import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class IntegrationTest extends TransactionalTestBase {

    private class PersonBulkInserter extends SqlServerBulkInsert<Person> {

        public PersonBulkInserter() {
            super("dbo", "UnitTest");

            mapString("FirstName", Person::getFirstName);
            mapString("LastName", Person::getLastName);
            mapDate("BirthDate", Person::getBirthDate);
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        // The Number of Entities to insert:
        int numEntities = 1000000;
        // Create a large list of Persons:
        List<Person> persons = getPersonList(numEntities);
        // Create the BulkInserter:
        PersonBulkInserter personBulkInserter = new PersonBulkInserter();
        // Now save all entities of a given stream:
        personBulkInserter.saveAll(connection, persons.stream());
        // And assert all have been written to the database:
        Assert.assertEquals(numEntities, getRowCount());
    }

    private List<Person> getPersonList(int numPersons) {
        List<Person> persons = new ArrayList<>();

        for (int pos = 0; pos < numPersons; pos++) {
            Person p = new Person();

            p.setFirstName("Philipp");
            p.setLastName("Wagner");
            p.setBirthDate(LocalDate.of(1986, 5, 12));

            persons.add(p);
        }

        return persons;
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = "CREATE TABLE [dbo].[UnitTest]\n" +
                "            (\n" +
                "                FirstName NVARCHAR(255),\n" +
                "                LastName NVARCHAR(255),\n" +
                "                BirthDate DATE\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private int getRowCount() throws SQLException {

        Statement s = connection.createStatement();

        ResultSet r = s.executeQuery("SELECT COUNT(*) AS total FROM [dbo].[UnitTest];");
        r.next();
        int count = r.getInt("total");
        r.close();

        return count;
    }
}
