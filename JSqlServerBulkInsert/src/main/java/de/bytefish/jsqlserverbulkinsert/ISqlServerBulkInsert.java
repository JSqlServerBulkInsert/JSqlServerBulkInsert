// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;

public interface ISqlServerBulkInsert<TEntity> {

    void saveAll(Connection connection, Stream<TEntity> entities) throws SQLException;

}
