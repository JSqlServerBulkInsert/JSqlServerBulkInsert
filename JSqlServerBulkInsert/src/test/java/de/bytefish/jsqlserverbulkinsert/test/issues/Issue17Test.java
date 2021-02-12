package de.bytefish.jsqlserverbulkinsert.test.issues;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.model.SchemaMetaData;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import de.bytefish.jsqlserverbulkinsert.test.model.Person;
import de.bytefish.jsqlserverbulkinsert.test.utils.MeasurementUtils;
import de.bytefish.jsqlserverbulkinsert.util.SchemaUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * https://github.com/JSqlServerBulkInsert/JSqlServerBulkInsert/issues/17
 */
public class Issue17Test extends TransactionalTestBase {

    // A Partial Mapping to check for Ordinal Correctness:
    public class PartialPersonMapping extends AbstractMapping<Person> {

        public PartialPersonMapping() {
            super("dbo", "UnitTest");

            mapNvarchar("FirstName", Person::getFirstName);
            mapDate("BirthDate", Person::getBirthDate);
            mapNvarchar("LastName", Person::getLastName);
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void informationSchemaTest() throws Exception {
        SchemaMetaData schemaMetaData = SchemaUtils.getInformationSchema(connection, "dbo", "UnitTest");

        Assert.assertEquals("FirstName", schemaMetaData.getColumns().get(0).getColumnName());
        Assert.assertEquals(1, schemaMetaData.getColumns().get(0).getOrdinal());

        Assert.assertEquals("LastName", schemaMetaData.getColumns().get(1).getColumnName());
        Assert.assertEquals(2, schemaMetaData.getColumns().get(1).getOrdinal());

        Assert.assertEquals("BirthDate", schemaMetaData.getColumns().get(2).getColumnName());
        Assert.assertEquals(3, schemaMetaData.getColumns().get(2).getOrdinal());
    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        // The Number of Entities to insert:
        int numEntities = 1000000;
        // Create a large list of Persons:
        List<Person> persons = getPersonList(numEntities);
        // Create the Mapping:
        PartialPersonMapping mapping = new PartialPersonMapping();
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

    private void getColumns() throws SQLException {
        String SPsql = "select COLUMN_NAME, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS";   // for stored proc taking 2 parameters
        PreparedStatement ps = connection.prepareStatement(SPsql);
        ResultSet rs = ps.executeQuery();
        if(rs.next()) {
            System.out.println(rs.getString("COLUMN_NAME") + ", " + rs.getInt("ORDINAL_POSITION"));
        }
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = "CREATE TABLE [dbo].[UnitTest]\n" +
                "            (\n" +
                "                FirstName NVARCHAR(255)\n" +
                "                , LastName NVARCHAR(255)\n" +
                "                , BirthDate DATE\n" +
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
