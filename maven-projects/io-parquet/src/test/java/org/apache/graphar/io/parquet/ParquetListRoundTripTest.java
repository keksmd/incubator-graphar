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

package org.apache.graphar.io.parquet;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.ValueVector;
import org.apache.graphar.io.VectorRecordBatch;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

/**
 * Verifies standard Parquet LIST values, empty lists, and null lists round-trip through neutral IO.
 */
public class ParquetListRoundTripTest {
    private static final Schema SCHEMA =
            new Schema(
                    List.of(
                            new Field("id", ColumnType.of(ColumnType.Kind.INT64), false),
                            new Field("payload", ColumnType.of(ColumnType.Kind.BINARY), true),
                            new Field(
                                    "tags",
                                    ColumnType.listOf(ColumnType.of(ColumnType.Kind.STRING)),
                                    true)));

    @Test
    public void roundTripsNullableStringLists() throws Exception {
        Path file = Files.createTempFile("graphar-list-", ".parquet");
        Files.deleteIfExists(file);
        LocalStorage storage = new LocalStorage();
        URI uri = file.toUri();
        try {
            new ParquetPhysicalWriter(storage)
                    .write(new WriteRequest(uri, SCHEMA, WriteMode.CREATE_NEW), writeBatch());
            List<Object> lists = new ArrayList<>();
            List<ByteBuffer> binaries = new ArrayList<>();
            try (BatchCursor cursor =
                    new ParquetPhysicalReader(storage)
                            .read(org.apache.graphar.io.ReadRequest.builder(uri).build())
                            .cursor()) {
                while (cursor.next()) {
                    RecordBatch batch = cursor.batch();
                    assertEquals(3, batch.columnCount());
                    assertEquals(SCHEMA.fields(), batch.schema().fields());
                    assertEquals(3, batch.column(0).valueCount());
                    assertEquals(false, batch.column(1).isNull(0));
                    assertEquals(true, batch.column(1).isNull(2));
                    for (int index = 0; index < batch.rowCount(); index++) {
                        lists.add(batch.column(2).getObject(index));
                        binaries.add((ByteBuffer) batch.column(1).getObject(index));
                    }
                }
            }
            assertEquals(List.of("one", "two"), lists.get(0));
            assertEquals(List.of(), lists.get(1));
            assertEquals(null, lists.get(2));
            assertEquals(ByteBuffer.wrap(new byte[] {1, 2, 3}), binaries.get(0));
            assertEquals(ByteBuffer.wrap(new byte[] {4, 5}), binaries.get(1));
            assertEquals(null, binaries.get(2));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static BatchCursor writeBatch() {
        RecordBatch batch =
                new VectorRecordBatch(
                        SCHEMA,
                        List.of(
                                new Values(SCHEMA.fields().get(0), List.of(1L, 2L, 3L)),
                                new Values(
                                        SCHEMA.fields().get(1),
                                        Arrays.asList(
                                                ByteBuffer.wrap(new byte[] {1, 2, 3}),
                                                ByteBuffer.wrap(new byte[] {4, 5}),
                                                null)),
                                new Values(
                                        SCHEMA.fields().get(2),
                                        Arrays.asList(List.of("one", "two"), List.of(), null))),
                        3);
        return new OneBatch(batch);
    }

    private static final class Values implements ValueVector {
        private final Field field;
        private final List<Object> values;

        private Values(Field field, List<Object> values) {
            this.field = field;
            this.values = values;
        }

        @Override
        public Field field() {
            return field;
        }

        @Override
        public int valueCount() {
            return values.size();
        }

        @Override
        public boolean isNull(int index) {
            return values.get(index) == null;
        }

        @Override
        public Object getObject(int index) {
            return values.get(index);
        }
    }

    private static final class OneBatch implements BatchCursor {
        private final RecordBatch batch;
        private boolean advanced;

        private OneBatch(RecordBatch batch) {
            this.batch = batch;
        }

        @Override
        public boolean next() {
            if (advanced) {
                return false;
            }
            advanced = true;
            return true;
        }

        @Override
        public RecordBatch batch() {
            return batch;
        }

        @Override
        public void close() {}
    }
}
