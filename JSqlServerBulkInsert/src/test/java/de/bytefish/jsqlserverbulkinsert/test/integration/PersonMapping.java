// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.integration;

import de.bytefish.jsqlserverbulkinsert.mapping.AbstractMapping;
import de.bytefish.jsqlserverbulkinsert.test.model.Person;

public class PersonMapping extends AbstractMapping<Person> {

    public PersonMapping() {
        super("dbo", "UnitTest");

        mapNvarchar("FirstName", Person::getFirstName);
        mapNvarchar("LastName", Person::getLastName);
        mapDate("BirthDate", Person::getBirthDate);
    }
}