package de.bytefish.jsqlserverbulkinsert.test.issues;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.model.SchemaMetaData;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import de.bytefish.jsqlserverbulkinsert.test.mapping.UTCNanoTest;
import de.bytefish.jsqlserverbulkinsert.test.model.Person;
import de.bytefish.jsqlserverbulkinsert.test.utils.MeasurementUtils;
import de.bytefish.jsqlserverbulkinsert.util.SchemaUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;

/**
 * https://github.com/JSqlServerBulkInsert/JSqlServerBulkInsert/issues/17
 */
public class Issue21Test extends TransactionalTestBase {

    public class Issue21Entity {

        public LocalDateTime lastUpdate;

        public Issue21Entity(LocalDateTime lastUpdate) {
            this.lastUpdate = lastUpdate;
        }

        public LocalDateTime getLastUpdate() {
            return lastUpdate;
        }
    }

    public class Issue21EntityMapping extends AbstractMapping<Issue21Entity> {

        public Issue21EntityMapping() {
            super("dbo", "UnitTest");

            mapLocalDateTime("LastUpdated", x -> x.getLastUpdate().truncatedTo(ChronoUnit.MILLIS));
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTestTable();
    }

    @Test
    public void bulkInsertToDateTimeColumn() throws SQLException {

        // Expected LocalDate 2017-05-15T12:09:07.161013600
        LocalDateTime current = LocalDateTime.now(Clock.systemUTC());

        // Create entities
        List<Issue21Entity> entities = Arrays.asList(new Issue21Entity (current));
        // Create the BulkInserter:
        Issue21EntityMapping mapping = new Issue21EntityMapping();
        // Now save all entities of a given stream:
        new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());

        // And assert all have been written to the database:
        ResultSet rs = getAll();
        while (rs.next()) {
            // for debugging purposes, can look at how the dates are stored in the DB
            String dbDate = rs.getString("lastUpdated");
            // Get the Date we have written:
            LocalDateTime date = rs.getObject("lastUpdated", LocalDateTime.class);

            // We should have a date:
            Assert.assertNotNull(date);

            Assert.assertEquals(current.truncatedTo(ChronoUnit.SECONDS), date.truncatedTo(ChronoUnit.SECONDS));
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
                "                lastUpdated datetime" +
                "            );";

        Statement statement = connection.createStatement();

        statement.execute(sqlStatement);
    }
}
