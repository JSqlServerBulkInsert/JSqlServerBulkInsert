// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.model;

public interface IColumnDefinition<TEntityType> {

    Object getPropertyValue(TEntityType entity);

    ColumnMetaData getColumnMetaData();

}

