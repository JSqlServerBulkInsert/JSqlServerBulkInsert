// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
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
import java.util.Arrays;
import java.util.List;

public class InstantTest extends TransactionalTestBase {

	private class TimestampEntity {

		private Instant localDate;

		private TimestampEntity(Instant localDate) {
			this.localDate = localDate;
		}

		public Instant getLocalDate() {
			return localDate;
		}
	}

	private class LocalDateEntityMapping extends AbstractMapping<TimestampEntity> {

		public LocalDateEntityMapping() {
			super("dbo", "UnitTest");

			mapInstant("instantcolumn", TimestampEntity::getLocalDate);
		}
	}

	@Override
	protected void onSetUpInTransaction() throws Exception {
		createTestTable();
	}

	@Test
	public void bulkInsertNowTest() throws SQLException {
		// Expected LocalDate (now)
		Instant localDate =  Instant.now();
		// Create entities
		List<TimestampEntity> entities = Arrays.asList(new TimestampEntity(localDate));
		// Create the BulkInserter:
		LocalDateEntityMapping mapping = new LocalDateEntityMapping();
		// Now save all entities of a given stream:
		new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());

		// And assert all have been written to the database:
		ResultSet rs = getAll();
		while (rs.next()) {
			// for debugging purposes, can look at how the dates are stored in the DB
			String localds = rs.getString("instantcolumn");
			// Get the Date we have written:
			Instant date = rs.getTimestamp("instantcolumn").toInstant();

			// We should have a date:
			Assert.assertNotNull(date);

			Assert.assertEquals(localDate.toString(), date.toString());
		}
	}

	@Test
	public void bulkInsertNullTest() throws SQLException {
		// Expected LocalDate null
		Instant localDate =  null;
		// Create entities
		List<TimestampEntity> entities = Arrays.asList(new TimestampEntity(localDate));
		// Create the BulkInserter:
		LocalDateEntityMapping mapping = new LocalDateEntityMapping();
		// Now save all entities of a given stream:
		new SqlServerBulkInsert<>(mapping).saveAll(connection, entities.stream());

		// And assert all have been written to the database:
		ResultSet rs = getAll();
		while (rs.next()) {
			// for debugging purposes, can look at how the dates are stored in the DB
			String localds = rs.getString("instantcolumn");
			// Get the Date we have written:
			Timestamp date = rs.getTimestamp("instantcolumn");

			Assert.assertEquals(null, date);
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
				"                instantcolumn datetime2\n" +
				"            );";

		Statement statement = connection.createStatement();

		statement.execute(sqlStatement);
	}
}