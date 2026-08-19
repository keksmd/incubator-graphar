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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class RecordBatchTest {

    private static Schema schema() {
        return new Schema(
                Arrays.asList(
                        new Field("flag", ColumnType.of(ColumnType.Kind.BOOLEAN), false),
                        new Field("count", ColumnType.of(ColumnType.Kind.INT64), false),
                        new Field("label", ColumnType.of(ColumnType.Kind.STRING), true),
                        new Field("blob", ColumnType.of(ColumnType.Kind.BINARY), false),
                        new Field("day", ColumnType.of(ColumnType.Kind.DATE), false),
                        new Field("at", ColumnType.of(ColumnType.Kind.TIMESTAMP_MILLIS), false),
                        new Field(
                                "tags",
                                ColumnType.listOf(ColumnType.of(ColumnType.Kind.STRING)),
                                false)));
    }

    private static List<Object> row() {
        return Arrays.asList(
                Boolean.TRUE,
                7L,
                null,
                ByteBuffer.wrap("gar".getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer(),
                LocalDate.of(2024, 5, 17),
                Instant.ofEpochMilli(1_715_000_000_000L),
                Arrays.asList("a", "b"));
    }

    @Test
    public void mapsEveryDocumentedColumnKindToItsNeutralJavaValue() throws IOException {
        try (ListBatchCursor cursor =
                new ListBatchCursor(
                        schema(), Collections.singletonList(Collections.singletonList(row())))) {
            assertTrue(cursor.next());
            Row first = cursor.batch().row(0);

            assertEquals(Boolean.TRUE, first.value(0));
            assertEquals(Long.valueOf(7L), first.value(1));
            assertNull(first.value(2));
            assertTrue(((ByteBuffer) first.value(3)).isReadOnly());
            assertEquals(LocalDate.of(2024, 5, 17), first.value(4));
            assertEquals(Instant.ofEpochMilli(1_715_000_000_000L), first.value(5));
            assertEquals(Arrays.asList("a", "b"), first.value(6));
        }
    }

    @Test
    public void publishesListValuesTheCallerCannotMutate() throws IOException {
        try (ListBatchCursor cursor =
                new ListBatchCursor(
                        schema(), Collections.singletonList(Collections.singletonList(row())))) {
            assertTrue(cursor.next());
            @SuppressWarnings("unchecked")
            List<String> tags = (List<String>) cursor.batch().row(0).value(6);

            assertThrows(UnsupportedOperationException.class, () -> tags.add("c"));
        }
    }

    @Test
    public void walksEveryBatchOnceAndThenReportsExhaustion() throws IOException {
        List<List<Object>> firstBatch = Arrays.asList(row(), row());
        List<List<Object>> secondBatch = Collections.singletonList(row());
        ListBatchCursor cursor =
                new ListBatchCursor(schema(), Arrays.asList(firstBatch, secondBatch));

        assertTrue(cursor.next());
        assertEquals(2, cursor.batch().rowCount());
        assertTrue(cursor.next());
        assertEquals(1, cursor.batch().rowCount());
        assertFalse(cursor.next());
        assertFalse(cursor.next());
        cursor.close();
    }

    @Test
    public void refusesToPublishABatchBeforeTheFirstAdvance() throws IOException {
        try (ListBatchCursor cursor =
                new ListBatchCursor(
                        schema(), Collections.singletonList(Collections.singletonList(row())))) {
            assertThrows(IllegalStateException.class, cursor::batch);
        }
    }

    @Test
    public void reportsTheSharedSchemaOnEveryBatch() throws IOException {
        Schema schema = schema();
        try (ListBatchCursor cursor =
                new ListBatchCursor(
                        schema,
                        Arrays.asList(
                                Collections.singletonList(row()),
                                Collections.singletonList(row())))) {
            assertTrue(cursor.next());
            assertEquals(schema, cursor.batch().schema());
            assertTrue(cursor.next());
            assertEquals(schema, cursor.batch().schema());
        }
    }
}
