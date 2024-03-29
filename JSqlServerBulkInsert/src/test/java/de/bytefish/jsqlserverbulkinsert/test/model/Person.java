// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.model;

import java.time.LocalDate;

public class Person {

    private String firstName;

    private String lastName;

    private LocalDate birthDate;

    private LocalDate retiringDate;

    public Person() {
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public LocalDate getBirthDate() {
        return birthDate;
    }

    public void setBirthDate(LocalDate birthDate) {
        this.birthDate = birthDate;
    }

    public LocalDate getRetiringDate() {
        return retiringDate;
    }

    public void setRetiringDate(LocalDate retiringDate) {
        this.retiringDate = retiringDate;
    }
}
