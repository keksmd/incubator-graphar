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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class SchemaTest {

    private static Field field(String name) {
        return new Field(name, ColumnType.of(ColumnType.Kind.INT64), false);
    }

    @Test
    public void keepsFieldsInPhysicalColumnOrder() {
        Schema schema = new Schema(Arrays.asList(field("src"), field("dst"), field("weight")));

        assertEquals(3, schema.fields().size());
        assertEquals("src", schema.fields().get(0).name());
        assertEquals("dst", schema.fields().get(1).name());
        assertEquals("weight", schema.fields().get(2).name());
    }

    @Test
    public void acceptsASchemaWithoutColumns() {
        assertTrue(new Schema(Collections.emptyList()).fields().isEmpty());
    }

    @Test
    public void preservesDuplicateColumnNamesForIndexBasedAccess() {
        Schema schema = new Schema(Arrays.asList(field("src"), field("src")));

        assertEquals(2, schema.fields().size());
        assertEquals("src", schema.fields().get(0).name());
        assertEquals("src", schema.fields().get(1).name());
    }

    @Test
    public void refusesAMissingFieldListOrField() {
        assertThrows(NullPointerException.class, () -> new Schema(null));
        assertThrows(
                NullPointerException.class, () -> new Schema(Arrays.asList(field("src"), null)));
    }

    @Test
    public void copiesTheSuppliedListSoLaterMutationCannotReachIt() {
        List<Field> supplied = new ArrayList<>();
        supplied.add(field("src"));
        Schema schema = new Schema(supplied);

        supplied.add(field("dst"));

        assertEquals(1, schema.fields().size());
    }

    @Test
    public void publishesAnUnmodifiableFieldList() {
        Schema schema = new Schema(Collections.singletonList(field("src")));

        assertThrows(UnsupportedOperationException.class, () -> schema.fields().add(field("dst")));
    }
}
