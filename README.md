[![Maven Central](https://img.shields.io/maven-central/v/de.bytefish/jsqlserverbulkinsert.svg?label=Maven%20Central)](https://search.maven.org/artifact/de.bytefish/jsqlserverbulkinsert)
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
	<version>5.1.0</version>
</dependency>
```

## Supported Types ##

Please read up the Microsoft Documentation for understanding the mapping between SQL Server Types and JDBC Data Types:

* [Understanding the JDBC Driver Data Types](https://docs.microsoft.com/en-us/sql/connect/jdbc/understanding-the-jdbc-driver-data-types)

The following JDBC Types are supported by the library:

* Numeric Types
    * TINYINT
    * SMALLINT
    * INTEGER
    * BIGINT
    * NUMERIC
	* REAL
    * DOUBLE
* Date/Time Types
    * DATE
    * TIMESTAMP
    * TIMESTAMP with Timezone
* Boolean Type
    * BIT
* Character / Text Types
    * CHAR
    * NCHAR
    * CLOB
    * VARCHAR
    * NVARCHAR
    * LONGVARCHAR
    * NLONGVARCHAR
* Binary Data Types
    * VARBINARY
   
   
## Notes on the Table Mapping ##

The ``SQLServerBulkCopy`` implementation of Microsoft requires **all columns** of the destination table 
to be defined, even if the columns contain auto-generated values.

So imagine you have a table with an auto-incrementing primary key:

```sql
CREATE TABLE [dbo].[UnitTest](
    PK_ID INT IDENTITY(1,1) PRIMARY KEY,
    IntegerValue INT
)
```

And although our class doesn't contain the Auto-Incrementing Primary Key:

```java
public class MySampleEntity {

    private final int val;

    public MySampleEntity(int val) {
        this.val = val;
    }

    public Integer getVal() {
        return val;
    }
}
```

We still need to map it in the AbstractMapping like this:

```java
public class MySampleEntityMapping extends AbstractMapping<MySampleEntity> {

    public MySampleEntityMapping() {
        super("dbo", "UnitTest");

        mapInteger("PK_ID", x -> null);
        mapInteger("IntegerValue", x -> x.getVal());
    }
}
```

Or like this to explicitly define the Column as auto-incrementing:

```java
private class MySampleEntityMapping extends AbstractMapping<MySampleEntity> {

    public MySampleEntityMapping() {
        super("dbo", "UnitTest");

        mapInteger("PK_ID", true);
        mapInteger("IntegerValue", x -> x.getVal());
    }
}
```

### Notes on DATETIME Columns ###

If you are trying to map a `LocalDateTime` to a `DATETIME` column, you need to drop the nanoseconds part. A SQL Server `DATETIME` column doesn't have this level of precision.

It can be fixed by using `LocalDateTime#truncatedTo(ChronoUnit.MILLIS)`, like this:

```java
public class Issue21EntityMapping extends AbstractMapping<Issue21Entity> {

    public Issue21EntityMapping() {
        super("dbo", "UnitTest");

        mapLocalDateTime("LastUpdated", x -> x.getLastUpdate().truncatedTo(ChronoUnit.MILLIS));
    }
}
```

### Order of Columns ###

The ``SqlServerBulkCopy`` implementation of the Microsoft JDBC driver requires, that the destination schema and 
mapping have same column order. This is done automatically by querying the metadata of the table and sorting your 
mappings, before inserting the data.

If this cannot be done automatically, because the JDBC driver does not return the metadata, **then the mapping and 
destination schema have to match, and the fields must be mapped in the same order as the destination table**.

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
package de.bytefish.jsqlserverbulkinsert.test.integration;

import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.model.Person;

public class PersonMapping extends AbstractMapping<Person> {

    public PersonMapping() {
        super("dbo", "UnitTest");

        mapNvarchar("FirstName", Person::getFirstName);
        mapNvarchar("LastName", Person::getLastName);
        mapDate("BirthDate", Person::getBirthDate);
    }
}
```

### Construct and use the SqlServerBulkInsert ###

The ``AbstractMapping`` is used to instantiate a ``SqlServerBulkInsert``, which provides 
a ``saveAll`` method to store a given stream of data.

```java
// Instantiate the SqlServerBulkInsert class:
SqlServerBulkInsert<Person> bulkInsert = new SqlServerBulkInsert<>(mapping);
// Now save all entities of a given stream:
bulkInsert.saveAll(connection, persons.stream());
```

And the full Integration Test:

```java
package de.bytefish.jsqlserverbulkinsert.test.integration;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import de.bytefish.jsqlserverbulkinsert.test.model.Person;
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
