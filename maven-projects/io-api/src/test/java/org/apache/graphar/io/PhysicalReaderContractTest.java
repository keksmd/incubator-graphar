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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class PhysicalReaderContractTest {
    @Test
    public void reportsTheExactIntersectionOfRequestedAndSupportedHints() throws IOException {
        ReadRequest request = requestWithEveryHint();
        RecordingReader reader =
                new RecordingReader(EnumSet.of(ReadCapability.PROJECTION, ReadCapability.LIMIT));

        ReadResult result = reader.read(request);

        assertSame(request, reader.request());
        assertEquals(
                EnumSet.of(ReadCapability.PROJECTION, ReadCapability.LIMIT),
                result.report().applied());
        assertEquals(
                EnumSet.of(ReadCapability.ROW_RANGE, ReadCapability.FILTER),
                result.report().declined());
    }

    @Test
    public void decliningAllHintsIsObservableAndStillAValidExactRead() throws IOException {
        ReadRequest request = requestWithEveryHint();
        RecordingReader reader = new RecordingReader(EnumSet.noneOf(ReadCapability.class));

        ReadResult result = reader.read(request);

        assertEquals(Collections.emptySet(), result.report().applied());
        assertEquals(EnumSet.allOf(ReadCapability.class), result.report().declined());
    }

    @Test
    public void declinedHintsPreserveRowsAndSchema() throws IOException {
        ReadRequest request = requestForRows();
        InMemoryReader allCapable = new InMemoryReader(EnumSet.allOf(ReadCapability.class));
        InMemoryReader noCapability = new InMemoryReader(EnumSet.noneOf(ReadCapability.class));

        Snapshot applied = snapshot(allCapable.read(request));
        Snapshot declined = snapshot(noCapability.read(request));

        assertEquals(applied, declined);
        assertEquals(List.of("id", "name"), declined.columnNames);
        assertEquals(List.of(List.of(2, "beta")), declined.rows);
    }

    @Test
    public void resultRejectsIncompleteOrUnrequestedReports() {
        ReadRequest request = ReadRequest.builder(URI.create("file:/input")).build();

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new ReadResult(
                                request,
                                EmptyBatchCursor.INSTANCE,
                                new ReadReport(
                                        EnumSet.of(ReadCapability.LIMIT), Collections.emptySet())));
    }

    private static ReadRequest requestWithEveryHint() {
        return ReadRequest.builder(URI.create("file:/input"))
                .projection(Projection.of(java.util.List.of("id")))
                .rowRange(new RowRange(10, 20))
                .filters(
                        java.util.List.of(
                                Filter.comparison(
                                        "id", ComparisonOperator.GREATER_THAN, Literal.of(7))))
                .limit(4)
                .build();
    }

    private static ReadRequest requestForRows() {
        return ReadRequest.builder(URI.create("memory:/input"))
                .projection(Projection.of(List.of("id", "name")))
                .rowRange(new RowRange(1, 5))
                .filters(
                        List.of(
                                Filter.comparison(
                                        "age",
                                        ComparisonOperator.GREATER_THAN_OR_EQUAL,
                                        Literal.of(18))))
                .limit(1)
                .build();
    }

    private static Snapshot snapshot(ReadResult result) throws IOException {
        List<String> columnNames = null;
        List<List<Object>> rows = new ArrayList<>();
        try (BatchCursor cursor = result.cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                if (columnNames == null) {
                    columnNames = new ArrayList<>();
                    for (Field field : batch.schema().fields()) {
                        columnNames.add(field.name());
                    }
                }
                for (int index = 0; index < batch.rowCount(); index++) {
                    List<Object> values = new ArrayList<>();
                    for (int column = 0; column < batch.columnCount(); column++) {
                        values.add(batch.column(column).getObject(index));
                    }
                    rows.add(List.copyOf(values));
                }
            }
        }
        return new Snapshot(List.copyOf(columnNames), List.copyOf(rows));
    }

    private static final class RecordingReader implements PhysicalReader {
        private final Set<ReadCapability> capabilities;
        private ReadRequest request;

        private RecordingReader(Set<ReadCapability> capabilities) {
            this.capabilities = Collections.unmodifiableSet(EnumSet.copyOf(capabilities));
        }

        @Override
        public Set<ReadCapability> capabilities() {
            return capabilities;
        }

        @Override
        public ReadResult read(ReadRequest request) {
            this.request = request;
            EnumSet<ReadCapability> applied = EnumSet.noneOf(ReadCapability.class);
            applied.addAll(request.requestedCapabilities());
            applied.retainAll(capabilities);
            EnumSet<ReadCapability> declined = EnumSet.noneOf(ReadCapability.class);
            declined.addAll(request.requestedCapabilities());
            declined.removeAll(applied);
            return new ReadResult(
                    request, EmptyBatchCursor.INSTANCE, new ReadReport(applied, declined));
        }

        private ReadRequest request() {
            return request;
        }
    }

    private static final class InMemoryReader implements PhysicalReader {
        private static final Schema INPUT_SCHEMA =
                new Schema(
                        List.of(
                                new Field("id", ColumnType.of(ColumnType.Kind.INT32), false),
                                new Field("name", ColumnType.of(ColumnType.Kind.STRING), false),
                                new Field("age", ColumnType.of(ColumnType.Kind.INT32), false)));
        private static final Schema OUTPUT_SCHEMA =
                new Schema(
                        List.of(
                                new Field("id", ColumnType.of(ColumnType.Kind.INT32), false),
                                new Field("name", ColumnType.of(ColumnType.Kind.STRING), false)));
        private static final List<List<Object>> INPUT_ROWS =
                List.of(
                        List.of(1, "alpha", 17),
                        List.of(2, "beta", 20),
                        List.of(3, "gamma", 25),
                        List.of(4, "delta", 16),
                        List.of(5, "epsilon", 18));

        private final Set<ReadCapability> capabilities;

        private InMemoryReader(Set<ReadCapability> capabilities) {
            this.capabilities = Collections.unmodifiableSet(EnumSet.copyOf(capabilities));
        }

        @Override
        public Set<ReadCapability> capabilities() {
            return capabilities;
        }

        @Override
        public ReadResult read(ReadRequest request) {
            EnumSet<ReadCapability> applied = reportedApplied(request);
            EnumSet<ReadCapability> declined = reportedDeclined(request, applied);
            List<List<Object>> projected = new ArrayList<>();
            RowRange range = request.rowRange().orElse(new RowRange(0, INPUT_ROWS.size()));
            for (long index = range.startInclusive(); index < range.endExclusive(); index++) {
                List<Object> row = INPUT_ROWS.get((int) index);
                if (((Integer) row.get(2)) >= 18) {
                    projected.add(List.of(row.get(0), row.get(1)));
                    if (request.limit().isPresent()
                            && projected.size() == request.limit().getAsLong()) {
                        break;
                    }
                }
            }
            return new ReadResult(
                    request,
                    new SingleBatchCursor(listRecordBatch(OUTPUT_SCHEMA, projected)),
                    new ReadReport(applied, declined));
        }

        private EnumSet<ReadCapability> reportedApplied(ReadRequest request) {
            EnumSet<ReadCapability> applied = EnumSet.noneOf(ReadCapability.class);
            applied.addAll(request.requestedCapabilities());
            applied.retainAll(capabilities);
            return applied;
        }

        private EnumSet<ReadCapability> reportedDeclined(
                ReadRequest request, Set<ReadCapability> applied) {
            EnumSet<ReadCapability> declined = EnumSet.noneOf(ReadCapability.class);
            declined.addAll(request.requestedCapabilities());
            declined.removeAll(applied);
            return declined;
        }
    }

    private static RecordBatch listRecordBatch(Schema schema, List<List<Object>> rows) {
        int columnCount = schema.fields().size();
        List<ValueVector> columns = new ArrayList<>(columnCount);
        for (int column = 0; column < columnCount; column++) {
            List<Object> values = new ArrayList<>(rows.size());
            for (List<Object> row : rows) {
                values.add(row.get(column));
            }
            columns.add(new ListValueVector(schema.fields().get(column), values));
        }
        return new VectorRecordBatch(schema, columns, rows.size());
    }

    private static final class ListValueVector implements ValueVector {
        private final Field field;
        private final List<Object> values;

        private ListValueVector(Field field, List<Object> values) {
            this.field = field;
            this.values = List.copyOf(values);
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

    private static final class SingleBatchCursor implements BatchCursor {
        private final RecordBatch batch;
        private boolean advanced;

        private SingleBatchCursor(RecordBatch batch) {
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
            if (!advanced) {
                throw new IllegalStateException("The cursor has no current batch.");
            }
            return batch;
        }

        @Override
        public void close() {}
    }

    private static final class Snapshot {
        private final List<String> columnNames;
        private final List<List<Object>> rows;

        private Snapshot(List<String> columnNames, List<List<Object>> rows) {
            this.columnNames = columnNames;
            this.rows = rows;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Snapshot)) {
                return false;
            }
            Snapshot that = (Snapshot) other;
            return columnNames.equals(that.columnNames) && rows.equals(that.rows);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(columnNames, rows);
        }
    }

    private enum EmptyBatchCursor implements BatchCursor {
        INSTANCE;

        @Override
        public boolean next() {
            return false;
        }

        @Override
        public RecordBatch batch() {
            throw new IllegalStateException("The cursor has no current batch.");
        }

        @Override
        public void close() {}
    }
}
