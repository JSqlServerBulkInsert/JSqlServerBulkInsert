// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.extensions.DateTimeExtensions;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

public class UTCNanoTest extends TransactionalTestBase {

	private class TimestampEntity {

		private Long utcNano;

		private TimestampEntity(Long localDate) {
			this.utcNano = localDate;
		}

		public Long getUtcNano() {
			return utcNano;
		}
	}

	private class LocalDateEntityMapping extends AbstractMapping<TimestampEntity> {

		public LocalDateEntityMapping() {
			super("dbo", "UnitTest");

			DateTimeExtensions.mapUTCNano(this, "utcnanocolumn", TimestampEntity::getUtcNano);
		}
	}

	@Override
	protected void onSetUpInTransaction() throws Exception {
		createTestTable();
	}

	@Test
	public void bulkInsertUTCTest() throws SQLException {

		// Expected LocalDate 2017-05-15T12:09:07.161013600
		long utcNanos = 1494850147161013648L;

		// Create entities
		List<TimestampEntity> entities = Arrays.asList(new TimestampEntity(utcNanos));
		// Create the BulkInserter:
		LocalDateEntityMapping mapping = new LocalDateEntityMapping();
		// Now save all entities of a given stream:
		new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());

		// And assert all have been written to the database:
		ResultSet rs = getAll();
		while (rs.next()) {
			// for debugging purposes, can look at how the dates are stored in the DB
			String dbDate = rs.getString("utcnanocolumn");
			// Get the Date we have written:
			LocalDateTime date = rs
					.getTimestamp("utcnanocolumn", Calendar.getInstance(TimeZone.getTimeZone("UTC")))
					.toInstant()
					.atZone(ZoneOffset.UTC)
					.toLocalDateTime();

			// We should have a date:
			Assert.assertNotNull(date);

			Assert.assertEquals("2017-05-15T12:09:07.161013600", date.toString());
		}
	}

	@Test
	public void bulkInsertNullTest() throws SQLException {
		// Expected: null
		Long utcNanos = null;
		// Create entities
		List<TimestampEntity> entities = Arrays.asList(new TimestampEntity(utcNanos));
		// Create the BulkInserter:
		LocalDateEntityMapping mapping = new LocalDateEntityMapping();
		// Now save all entities of a given stream:
		new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());

		// And assert all have been written to the database:
		ResultSet rs = getAll();
		while (rs.next()) {
			// for debugging purposes, can look at how the dates are stored in the DB
			String dbDate = rs.getString("utcnanocolumn");
			// Get the Date we have written:
			Timestamp timeStampResult = rs.getTimestamp("utcnanocolumn");
			Timestamp date = timeStampResult;

			Assert.assertEquals(null, dbDate);
			Assert.assertEquals(null, timeStampResult);
			Assert.assertEquals(null, date);
		}
	}

	private ResultSet getAll() throws SQLException {

		String sqlStatement = "SELECT utcnanocolumn  AT TIME ZONE 'UTC' AS [utcnanocolumn] FROM dbo.UnitTest";

		Statement statement = connection.createStatement();

		return statement.executeQuery(sqlStatement);
	}

	private void createTestTable() throws SQLException {
		String sqlStatement = "CREATE TABLE [dbo].[UnitTest]\n" +
				"            (\n" +
				"                utcnanocolumn datetime2\n" +
				"            );";

		Statement statement = connection.createStatement();

		statement.execute(sqlStatement);
	}
}