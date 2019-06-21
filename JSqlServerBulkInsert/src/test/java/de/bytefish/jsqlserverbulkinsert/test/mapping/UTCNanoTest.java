// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

import de.bytefish.jsqlserverbulkinsert.SqlServerBulkInsert;
import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.base.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

public class UTCNanoTest extends TransactionalTestBase {

	private class TimestampEntity {

		private long localDate;

		private TimestampEntity(long localDate) {
			this.localDate = localDate;
		}

		public long getLocalDate() {
			return localDate;
		}
	}

	private class LocalDateEntityMapping extends AbstractMapping<TimestampEntity> {

		public LocalDateEntityMapping() {
			super("dbo", "UnitTest");

			mapUTCNano("utcnanocolumn", TimestampEntity::getLocalDate);
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
			Timestamp timeStampResult = rs.getTimestamp("utcnanocolumn");
			LocalDateTime date = timeStampResult.toLocalDateTime();

			// We should have a date:
			Assert.assertNotNull(date);

			Assert.assertEquals("2017-05-15 12:09:07.1610136", dbDate);
			Assert.assertEquals("2017-05-15 12:09:07.1610136", timeStampResult.toString());
			Assert.assertEquals("2017-05-15T12:09:07.161013600", date.toString());
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
				"                utcnanocolumn datetime2\n" +
				"            );";

		Statement statement = connection.createStatement();

		statement.execute(sqlStatement);
	}
}