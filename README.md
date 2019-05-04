# JSqlServerBulkInsert #

[JSqlServerBulkInsert]: https://github.com/bytefish/JSqlServerBulkInsert
[MIT License]: https://opensource.org/licenses/MIT

[JSqlServerBulkInsert] is a library to simplify Bulk Inserts to the SQL Server. It wraps the ``SQLServerBulkCopy`` behind a nice API.

## Installing ##

You can obtain [JSqlServerBulkInsert] from Maven by adding the following:

```xml
<dependency>
	<groupId>de.bytefish</groupId>
	<artifactId>jsqlserverbulkinsert</artifactId>
	<version>1.4</version>
</dependency>
```

## Getting Started ##

Imagine ``1,000,000`` Persons should be inserted into an SQL Server database.

### Results ###

Bulk Inserting ``1,000,000``entities to a SQL Server 2016 database took ``5`` Seconds:

```
[Bulk Insert 1000000 Entities] PT4.559S
```

### Domain Model ###

The domain model could be the ``Person`` class with a First Name, Last Name and a birth date. 

```java
// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.model;

import java.time.LocalDate;

public class Person {

    private String firstName;

    private String lastName;

    private LocalDate birthDate;

    public Person() {
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public LocalDate getBirthDate() {
        return birthDate;
    }

    public void setBirthDate(LocalDate birthDate) {
        this.birthDate = birthDate;
    }
}
```

### Mapping ###

To bulk insert the ``Person`` data to a SQL Server database it is important to know how to map 
between the Java Object and the Database Columns:

```java
// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.integration;

import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.model.Person;

public class PersonMapping extends AbstractMapping<Person> {

    public PersonMapping() {
        super("dbo", "UnitTest");

        mapString("FirstName", Person::getFirstName);
        mapString("LastName", Person::getLastName);
        mapDate("BirthDate", Person::getBirthDate);
    }
}
```

### Construct and use the SqlServerBulkInsert ###

The ``AbstractMapping`` is used to instantiate a ``SqlServerBulkInsert``, which provides a ``saveAll`` method to store a given stream of data.

```java
// Instantiate the SqlServerBulkInsert class:
SqlServerBulkInsert<Person> bulkInsert = new SqlServerBulkInsert<>(mapping);
// Now save all entities of a given stream:
bulkInsert.saveAll(connection, persons.stream());
```

And the full Integration Test:

```java
// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.integration;

import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.model.Person;
import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import de.bytefish.jsqlserverbulkinsert.test.utils.MeasurementUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class IntegrationTest extends TransactionalTestBase {

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
        // Create the Mapping:
        PersonMapping mapping = new PersonMapping();
        // Create the Bulk Inserter:
        SqlServerBulkInsert<Person> bulkInsert = new SqlServerBulkInsert<>(mapping);
        // Measure the Bulk Insert time:
        MeasurementUtils.MeasureElapsedTime("Bulk Insert 1000000 Entities", () -> {
            // Now save all entities of a given stream:
            bulkInsert.saveAll(connection, persons.stream());
        });
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
```
