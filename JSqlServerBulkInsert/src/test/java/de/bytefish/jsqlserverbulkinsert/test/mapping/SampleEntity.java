// Copyright (c) Philipp Wagner and Victor Lee. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.jsqlserverbulkinsert.test.mapping;

public abstract class SampleEntity<TProperty> {

    private final TProperty value;

    public SampleEntity(TProperty value) {
        this.value = value;
    }

    public TProperty getValue() {
        return value;
    }
}
