// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

public class LocalDateTimeTest extends TransactionalTestBase {

	private class LocalDateTimeEntity {

		private LocalDateTime localDate;
		private LocalDateTime utcDate;

		private LocalDateTimeEntity(LocalDateTime localDate, LocalDateTime utcDate) {
			this.localDate = localDate;
			this.utcDate = utcDate;
		}

		public LocalDateTime getLocalDate() {
			return localDate;
		}

		public LocalDateTime getUTCDate() {
			return utcDate;
		}
	}

	private class LocalDateEntityMapping extends AbstractMapping<LocalDateTimeEntity> {

		public LocalDateEntityMapping() {
			super("dbo", "UnitTest");

			mapLocalDateTime("localtimestampcolumn", LocalDateTimeEntity::getLocalDate);
			mapLocalDateTime("utctimestampcolumn", LocalDateTimeEntity::getUTCDate);
		}
	}

	@Override
	protected void onSetUpInTransaction() throws Exception {
		createTestTable();
	}

	@Test
	public void bulkInsertNowTest() throws SQLException {
		// Expected LocalDate (now)
		LocalDateTime localDate = LocalDateTime.now();
		// Expected UTCDate (now)
		LocalDateTime utcDate = localDate.atZone(ZoneId.systemDefault()).withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
		// Create entities
		List<LocalDateTimeEntity> entities = Arrays.asList(new LocalDateTimeEntity(localDate, utcDate));
		// Create the BulkInserter:
		LocalDateEntityMapping mapping = new LocalDateEntityMapping();
		// Now save all entities of a given stream:
		new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());

		// And assert all have been written to the database:
		ResultSet rs = getAll();
		while (rs.next()) {
			// for debugging purposes, can look at how the dates are stored in the DB
			String localds = rs.getString("localtimestampcolumn");
			String utcds = rs.getString("utctimestampcolumn");
			// Get the Date we have written:
			LocalDateTime retreivedLocalDate = rs.getTimestamp("localtimestampcolumn").toLocalDateTime();
			LocalDateTime retreivedUTCDate = rs.getTimestamp("utctimestampcolumn").toLocalDateTime();

			// We should have a date:
			Assert.assertNotNull(retreivedLocalDate);
			Assert.assertNotNull(retreivedUTCDate);

			Assert.assertEquals(localDate.toString(), retreivedLocalDate.toString());
			Assert.assertEquals(utcDate.toString(), retreivedUTCDate.toString());
		}
	}

	@Test
	public void bulkInsertSetPointTest() throws SQLException {
		// Expected LocalDate (now)
		LocalDateTime localDate = LocalDateTime.now();
		// Expected UTCDate: Create a set point at 2017-05-15T12:09:07.161013600 (UTC)
		long seconds = 1494850147;
		int nanoseconds = 161013600;
		nanoseconds = (nanoseconds/100)*100; // round to 100 nanoseconds (precision that SQL server can handle)
		LocalDateTime utcDate = LocalDateTime.ofEpochSecond(seconds,
				nanoseconds,
				ZoneOffset.UTC);
		// Create entities
		List<LocalDateTimeEntity> entities = Arrays.asList(new LocalDateTimeEntity(localDate, utcDate));
		// Create the BulkInserter:
		LocalDateEntityMapping mapping = new LocalDateEntityMapping();
		// Now save all entities of a given stream:
		new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());

		// And assert all have been written to the database:
		ResultSet rs = getAll();
		while (rs.next()) {
			// for debugging purposes, can look at how the dates are stored in the DB
			String localds = rs.getString("localtimestampcolumn");
			String utcds = rs.getString("utctimestampcolumn");
			// Get the Date we have written:
			LocalDateTime retreivedLocalDate = rs.getObject("localtimestampcolumn", LocalDateTime.class);
			LocalDateTime retreivedUTCDate = rs.getTimestamp("utctimestampcolumn", Calendar.getInstance(TimeZone.getTimeZone("UTC")))
					.toInstant()
					.atZone(ZoneOffset.UTC)
					.toLocalDateTime();

			// We should have a date:
			Assert.assertNotNull(retreivedLocalDate);
			Assert.assertNotNull(retreivedUTCDate);

			Assert.assertEquals(localDate, retreivedLocalDate);
			Assert.assertEquals(utcDate, retreivedUTCDate);
		}
	}

	@Test
	public void bulkInsertNullTest() throws SQLException {
		// Expected LocalDate (now)
		LocalDateTime localDate = LocalDateTime.now();
		// Expected UTCDate: Create a null
		LocalDateTime utcDate = null;
		// Create entities
		List<LocalDateTimeEntity> entities = Arrays.asList(new LocalDateTimeEntity(localDate, utcDate));
		// Create the BulkInserter:
		LocalDateEntityMapping mapping = new LocalDateEntityMapping();
		// Now save all entities of a given stream:
		new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());

		// And assert all have been written to the database:
		ResultSet rs = getAll();
		while (rs.next()) {
			// for debugging purposes, can look at how the dates are stored in the DB
			String localds = rs.getString("localtimestampcolumn");
			String utcds = rs.getString("utctimestampcolumn");
			// Get the Date we have written:
			LocalDateTime retrievedLocalDate = rs.getObject("localtimestampcolumn", LocalDateTime.class);
			LocalDateTime retreivedUTCDate = rs.getObject("utctimestampcolumn", LocalDateTime.class);

			// We should have a date:
			Assert.assertNotNull(retrievedLocalDate);

			Assert.assertEquals(localDate, retrievedLocalDate);
			Assert.assertEquals(null, retreivedUTCDate);
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
				"                localtimestampcolumn datetime2,\n" +
				"                utctimestampcolumn datetime2\n" +
				"            );";

		Statement statement = connection.createStatement();

		statement.execute(sqlStatement);
	}
}