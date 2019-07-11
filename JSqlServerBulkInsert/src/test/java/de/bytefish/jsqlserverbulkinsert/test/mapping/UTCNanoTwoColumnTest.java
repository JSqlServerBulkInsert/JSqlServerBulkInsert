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
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;

public class UTCNanoTwoColumnTest extends TransactionalTestBase {

	private class TimestampEntity {

		private Long utcNano;

		private TimestampEntity(Long localDate) {
			this.utcNano = localDate;
		}

		public Long getUTCNano() {
			return utcNano;
		}
	}

	private class LocalDateEntityMapping extends AbstractMapping<TimestampEntity> {

		public LocalDateEntityMapping() {
			super("dbo", "UnitTest");

			mapUTCNano("datecolumn", "timecolumn", TimestampEntity::getUTCNano);
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
			String dbDate = rs.getString("datecolumn");
			String dbTime = rs.getString("timecolumn");
			// Get the Date we have written:
			Date dateResult = rs.getDate("datecolumn");
			Time timeResult = rs.getTime("timecolumn");
			LocalTime morePreciseTime = LocalTime.parse(dbTime);
			LocalDateTime dt = LocalDateTime.of(dateResult.toLocalDate(), morePreciseTime);

			// We should have a date:
			Assert.assertNotNull(dateResult);
			Assert.assertNotNull(timeResult);

			Assert.assertEquals("2017-05-15", dbDate);
			Assert.assertEquals("12:09:07.1610136", dbTime);
			Assert.assertEquals("2017-05-15", dateResult.toString());
			Assert.assertEquals("12:09:07", timeResult.toString());
			Assert.assertEquals("2017-05-15T12:09:07.161013600", dt.toString());
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
			String dbDate = rs.getString("datecolumn");
			String dbTime = rs.getString("timecolumn");
			// Get the Date we have written:
			Date dateResult = rs.getDate("datecolumn");
			Time timeResult = rs.getTime("timecolumn");

			Assert.assertEquals(null, dbDate);
			Assert.assertEquals(null, dbTime);
			Assert.assertEquals(null, dateResult);
			Assert.assertEquals(null, timeResult);
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
				"                datecolumn date,\n" +
				"                timecolumn time\n" +
				"            );";

		Statement statement = connection.createStatement();

		statement.execute(sqlStatement);
	}
}
