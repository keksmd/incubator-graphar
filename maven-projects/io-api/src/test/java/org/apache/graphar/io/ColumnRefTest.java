/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.graphar.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import java.util.List;
import org.junit.Test;

public class ColumnRefTest {
    private static final Field ID = new Field("id", ColumnType.of(ColumnType.Kind.INT32), false);
    private static final Field NAME =
            new Field("name", ColumnType.of(ColumnType.Kind.STRING), true);

    @Test
    public void comparesByExactName() {
        assertEquals(ColumnRef.of("id"), ColumnRef.of("id"));
        assertEquals(ColumnRef.of("id").hashCode(), ColumnRef.of("id").hashCode());
        assertNotEquals(ColumnRef.of("id"), ColumnRef.of("Id"));
        assertNotEquals(ColumnRef.of("id"), ColumnRef.of("id "));
        assertEquals("id", ColumnRef.of("id").name());
    }

    @Test
    public void refusesABlankName() {
        assertThrows(IllegalArgumentException.class, () -> ColumnRef.of(null));
        assertThrows(IllegalArgumentException.class, () -> ColumnRef.of(""));
        assertThrows(IllegalArgumentException.class, () -> ColumnRef.of("  "));
    }

    @Test
    public void resolvesToThePhysicalIndexOfTheOnlyMatchingField() {
        Schema schema = new Schema(List.of(ID, NAME));

        assertEquals(0, schema.resolve(ColumnRef.of("id")));
        assertEquals(1, schema.resolve(ColumnRef.of("name")));
    }

    @Test
    public void refusesToGuessAnUnknownOrAmbiguousColumn() {
        Schema unique = new Schema(List.of(ID, NAME));
        Schema duplicated = new Schema(List.of(ID, NAME, NAME));

        assertThrows(IllegalArgumentException.class, () -> unique.resolve(ColumnRef.of("age")));
        assertThrows(IllegalArgumentException.class, () -> unique.resolve(ColumnRef.of("ID")));
        assertEquals(0, duplicated.resolve(ColumnRef.of("id")));
        assertThrows(
                IllegalArgumentException.class, () -> duplicated.resolve(ColumnRef.of("name")));
    }
}
