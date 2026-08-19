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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class FieldTest {

    @Test
    public void carriesItsNameTypeAndNullability() {
        ColumnType type = ColumnType.of(ColumnType.Kind.STRING);
        Field nullable = new Field("id", type, true);
        Field required = new Field("id", type, false);

        assertEquals("id", nullable.name());
        assertEquals(type, nullable.type());
        assertTrue(nullable.nullable());
        assertFalse(required.nullable());
    }

    @Test
    public void refusesABlankName() {
        ColumnType type = ColumnType.of(ColumnType.Kind.STRING);

        assertThrows(IllegalArgumentException.class, () -> new Field(null, type, true));
        assertThrows(IllegalArgumentException.class, () -> new Field("", type, true));
        assertThrows(IllegalArgumentException.class, () -> new Field("   ", type, true));
    }

    @Test
    public void refusesAMissingType() {
        assertThrows(NullPointerException.class, () -> new Field("id", null, true));
    }
}
