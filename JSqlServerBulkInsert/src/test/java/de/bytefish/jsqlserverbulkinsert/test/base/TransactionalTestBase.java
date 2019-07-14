// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.base;


import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public abstract class TransactionalTestBase {

    protected Connection connection;

    @Before
    public void setUp() throws Exception {
        Properties properties = getConfigProperties();

        String dbUrl = properties.getProperty("db.url");
        String dbUser = properties.getProperty("db.user");
        String dbPassword = properties.getProperty("db.password");

        connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);

        onSetUpBeforeTransaction();
        connection.setAutoCommit(false); // Start the Transaction:
        onSetUpInTransaction();
    }

    @After
    public void tearDown() throws Exception {

        onTearDownInTransaction();
        connection.rollback();
        onTearDownAfterTransaction();

        connection.close();
    }

    protected void onSetUpInTransaction() throws Exception {}

    protected void onSetUpBeforeTransaction() throws Exception {}

    protected void onTearDownInTransaction() throws Exception {}

    protected void onTearDownAfterTransaction() throws Exception {}

    private static Properties getConfigProperties() throws Exception {
        try(InputStream inputStream = TransactionalTestBase.class.getClassLoader().getResourceAsStream("config.properties")) {

            Properties prop = new Properties();

            prop.load(inputStream);

            return prop;
        }
    }
}
