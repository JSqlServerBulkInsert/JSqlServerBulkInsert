package de.bytefish.jsqlserverbulkinsert.test.issues;

import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.model.InformationSchema;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import de.bytefish.jsqlserverbulkinsert.test.integration.PersonMapping;
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

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void informationSchemaTest() throws Exception {
        List<InformationSchema.ColumnInformation> columnInformations = SchemaUtils.getColumnInformations(connection, "dbo", "UnitTest");

        Assert.assertEquals("FirstName", columnInformations.get(0).getColumnName());
        Assert.assertEquals(1, columnInformations.get(0).getOrdinal());

        Assert.assertEquals("LastName", columnInformations.get(1).getColumnName());
        Assert.assertEquals(2, columnInformations.get(1).getOrdinal());

        Assert.assertEquals("BirthDate", columnInformations.get(2).getColumnName());
        Assert.assertEquals(3, columnInformations.get(2).getOrdinal());
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
