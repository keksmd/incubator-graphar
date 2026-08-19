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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Pins the obligations every {@link PhysicalWriter} owes its caller, against an in-memory reference
 * implementation that stands in for a real format backend.
 */
public class PhysicalWriterContractTest {

    private static final URI TARGET = URI.create("memory:/vertex/person/chunk0");

    private static Schema schema() {
        return new Schema(
                Collections.singletonList(
                        new Field("id", ColumnType.of(ColumnType.Kind.INT64), false)));
    }

    private static ListBatchCursor cursorOf(long... ids) {
        List<List<Object>> rows = new ArrayList<>();
        for (long id : ids) {
            rows.add(Collections.singletonList(id));
        }
        return new ListBatchCursor(schema(), Collections.singletonList(rows));
    }

    private static final class MemoryWriter implements PhysicalWriter {
        private final Map<URI, List<Object>> written = new HashMap<>();

        @Override
        public void write(WriteRequest request, BatchCursor batches) throws IOException {
            if (request.mode() == WriteMode.CREATE_NEW && written.containsKey(request.uri())) {
                throw new IOException("Target already exists: " + request.uri());
            }
            List<Object> values = new ArrayList<>();
            while (batches.next()) {
                RecordBatch batch = batches.batch();
                if (!describes(batch.schema(), request.schema())) {
                    throw new IOException("A batch does not match the requested schema.");
                }
                for (int row = 0; row < batch.rowCount(); row++) {
                    values.add(batch.row(row).value(0));
                }
            }
            written.put(request.uri(), values);
        }

        private static boolean describes(Schema batchSchema, Schema requested) {
            List<Field> left = batchSchema.fields();
            List<Field> right = requested.fields();
            if (left.size() != right.size()) {
                return false;
            }
            for (int column = 0; column < left.size(); column++) {
                if (!left.get(column).name().equals(right.get(column).name())
                        || !left.get(column).type().equals(right.get(column).type())) {
                    return false;
                }
            }
            return true;
        }
    }

    @Test
    public void writesEveryRowOfEveryBatchToTheRequestedTarget() throws IOException {
        MemoryWriter writer = new MemoryWriter();

        writer.write(
                new WriteRequest(TARGET, schema(), WriteMode.CREATE_NEW), cursorOf(1L, 2L, 3L));

        assertEquals(Arrays.asList(1L, 2L, 3L), writer.written.get(TARGET));
    }

    @Test
    public void refusesToReplaceAnExistingTargetUnderCreateNew() throws IOException {
        MemoryWriter writer = new MemoryWriter();
        writer.write(new WriteRequest(TARGET, schema(), WriteMode.CREATE_NEW), cursorOf(1L));

        IOException failure =
                assertThrows(
                        IOException.class,
                        () ->
                                writer.write(
                                        new WriteRequest(TARGET, schema(), WriteMode.CREATE_NEW),
                                        cursorOf(2L)));

        assertTrue(failure.getMessage().contains(TARGET.toString()));
        assertEquals(Collections.singletonList(1L), writer.written.get(TARGET));
    }

    @Test
    public void replacesAnExistingTargetUnderOverwrite() throws IOException {
        MemoryWriter writer = new MemoryWriter();
        writer.write(new WriteRequest(TARGET, schema(), WriteMode.CREATE_NEW), cursorOf(1L));

        writer.write(new WriteRequest(TARGET, schema(), WriteMode.OVERWRITE), cursorOf(2L, 3L));

        assertEquals(Arrays.asList(2L, 3L), writer.written.get(TARGET));
    }

    @Test
    public void acceptsAnEmptyCursorAndStillCreatesTheTarget() throws IOException {
        MemoryWriter writer = new MemoryWriter();

        writer.write(
                new WriteRequest(TARGET, schema(), WriteMode.CREATE_NEW),
                new ListBatchCursor(schema(), Collections.emptyList()));

        assertEquals(Collections.emptyList(), writer.written.get(TARGET));
    }

    @Test
    public void refusesABatchThatDoesNotMatchTheRequestedSchema() {
        MemoryWriter writer = new MemoryWriter();
        Schema other =
                new Schema(
                        Collections.singletonList(
                                new Field("other", ColumnType.of(ColumnType.Kind.INT64), false)));

        assertThrows(
                IOException.class,
                () ->
                        writer.write(
                                new WriteRequest(TARGET, other, WriteMode.CREATE_NEW),
                                cursorOf(1L)));
    }

    @Test
    public void leavesTheSuppliedCursorForItsOwnerToClose() throws IOException {
        MemoryWriter writer = new MemoryWriter();
        ListBatchCursor cursor = cursorOf(1L, 2L);

        writer.write(new WriteRequest(TARGET, schema(), WriteMode.CREATE_NEW), cursor);

        assertEquals(0, cursor.closeCount());
        cursor.close();
        assertEquals(1, cursor.closeCount());
    }
}
