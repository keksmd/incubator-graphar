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

package org.apache.graphar.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.graphar.info.AdjacentList;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadCapability;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Row;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.io.Schema;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Generated streaming contract for edge topology/property physical joins. */
public class EdgePropertyCursorStreamingTest {
    private static final int ROWS = 10_000;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void joinsLargeChunksInLockstepWithoutMaterializingThem() throws Exception {
        EdgeInfo edge = edge(AdjListType.ordered_by_source);
        CountingPhysicalReader reader = new CountingPhysicalReader();
        EdgePropertyCursor cursor =
                new EdgePropertyCursor(
                        edge,
                        AdjListType.ordered_by_source,
                        URI.create("file:/generated/"),
                        reader,
                        List.of(new EdgePropertyCursor.Segment(0, 0, new RowRange(0, ROWS))),
                        null,
                        List.of("weight"),
                        ROWS);

        assertTrue(cursor.next());
        assertEquals(0L, cursor.edge().source());
        assertEquals(0.0D, cursor.edge().properties().get("weight"));
        assertEquals(Set.of("weight"), cursor.edge().properties().keySet());
        assertEquals(2, reader.requests.size());
        for (CountingBatchCursor source : reader.cursors) {
            assertEquals(
                    "First edge must require only the first physical batch.", 1, source.nextCalls);
            assertTrue(source.totalBatchCount() > 1);
        }

        cursor.close();
        for (CountingBatchCursor source : reader.cursors) {
            assertTrue(source.closed);
        }
        assertFalse(reader.requestedUriContains("kind"));
    }

    @Test
    public void appliesProjectionAndLimitToExactOrderedPhysicalRange() throws Exception {
        Path root = temporaryFolder.newFolder("ordered").toPath();
        EdgeInfo edge = edge(AdjListType.ordered_by_source);
        writeLong(root, edge.getVerticesNumFileUri(AdjListType.ordered_by_source), 2);
        writeLong(root, edge.getEdgesNumFileUri(AdjListType.ordered_by_source, 0), ROWS);
        CountingPhysicalReader physicalReader = new CountingPhysicalReader();
        EdgeLayoutReader reader =
                new EdgeLayoutReader(
                        edge,
                        AdjListType.ordered_by_source,
                        root.toUri(),
                        new LocalStorage(),
                        physicalReader);

        try (EdgePropertyCursor cursor = reader.edges(1, List.of("weight"), 5)) {
            int seen = 0;
            while (cursor.next()) {
                assertEquals(10L + seen, cursor.edge().source());
                assertEquals(Set.of("weight"), cursor.edge().properties().keySet());
                seen++;
            }
            assertEquals(5, seen);
        }

        assertEquals(3, physicalReader.requests.size());
        assertEquals(new RowRange(1, 3), physicalReader.requests.get(0).rowRange().orElseThrow());
        assertEquals(new RowRange(10, 15), physicalReader.requests.get(1).rowRange().orElseThrow());
        assertEquals(new RowRange(10, 15), physicalReader.requests.get(2).rowRange().orElseThrow());
        assertEquals(List.of("weight"), physicalReader.requests.get(2).projection().columns());
        assertFalse(physicalReader.requestedUriContains("kind"));
    }

    @Test
    public void scansOnlyTheUnorderedPartitionThenStopsAtTheOutputLimit() throws Exception {
        Path root = temporaryFolder.newFolder("unordered").toPath();
        EdgeInfo edge = edge(AdjListType.unordered_by_source);
        writeLong(root, edge.getVerticesNumFileUri(AdjListType.unordered_by_source), 50);
        writeLong(root, edge.getEdgesNumFileUri(AdjListType.unordered_by_source, 0), ROWS);
        CountingPhysicalReader physicalReader = new CountingPhysicalReader();
        EdgeLayoutReader reader =
                new EdgeLayoutReader(
                        edge,
                        AdjListType.unordered_by_source,
                        root.toUri(),
                        new LocalStorage(),
                        physicalReader);

        try (EdgePropertyCursor cursor = reader.edges(13, List.of("weight"), 3)) {
            int seen = 0;
            while (cursor.next()) {
                assertEquals(13L, cursor.edge().source());
                seen++;
            }
            assertEquals(3, seen);
        }

        assertEquals(2, physicalReader.requests.size());
        for (ReadRequest request : physicalReader.requests) {
            assertTrue(request.uri().getPath().contains("part0"));
            assertEquals(new RowRange(0, ROWS), request.rowRange().orElseThrow());
        }
        assertTrue(
                "Filtering an unordered partition stops before all physical batches are consumed.",
                physicalReader.cursors.get(0).nextCalls
                        < physicalReader.cursors.get(0).totalBatchCount());
    }

    private static EdgeInfo edge(AdjListType layout) {
        PropertyGroup weights =
                new PropertyGroup(
                        List.of(new Property("weight", DataType.DOUBLE, false, false)),
                        FileType.PARQUET,
                        "weight/");
        PropertyGroup kinds =
                new PropertyGroup(
                        List.of(new Property("kind", DataType.STRING, false, false)),
                        FileType.PARQUET,
                        "kind/");
        return new EdgeInfo(
                "person",
                "knows",
                "person",
                ROWS,
                ROWS,
                ROWS,
                true,
                URI.create("edge/person_knows_person/"),
                "gar/v1",
                List.of(new AdjacentList(layout, FileType.PARQUET, layout + "/")),
                List.of(weights, kinds));
    }

    private static void writeLong(Path root, URI relative, long value) throws IOException {
        Path path = root.resolve(relative.toString());
        Files.createDirectories(path.getParent());
        Files.write(
                path,
                ByteBuffer.allocate(Long.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putLong(value)
                        .array());
    }

    private static final class CountingPhysicalReader implements PhysicalReader {
        private final List<ReadRequest> requests = new ArrayList<>();
        private final List<CountingBatchCursor> cursors = new ArrayList<>();

        @Override
        public Set<ReadCapability> capabilities() {
            return Collections.unmodifiableSet(EnumSet.allOf(ReadCapability.class));
        }

        @Override
        public ReadResult read(ReadRequest request) {
            requests.add(request);
            CountingBatchCursor cursor = new CountingBatchCursor(request);
            cursors.add(cursor);
            return new ReadResult(
                    request,
                    cursor,
                    new ReadReport(request.requestedCapabilities(), Collections.emptySet()));
        }

        private boolean requestedUriContains(String part) {
            return requests.stream().anyMatch(request -> request.uri().getPath().contains(part));
        }
    }

    private static final class CountingBatchCursor implements BatchCursor {
        private final ReadRequest request;
        private final long start;
        private final long end;
        private long next;
        private int nextCalls;
        private boolean closed;
        private RecordBatch current;

        private CountingBatchCursor(ReadRequest request) {
            this.request = request;
            RowRange range = request.rowRange().orElse(new RowRange(0, ROWS));
            this.start = range.startInclusive();
            this.end = range.endExclusive();
            this.next = start;
        }

        @Override
        public boolean next() {
            nextCalls++;
            if (closed || next == end) {
                current = null;
                return false;
            }
            long batchStart = next;
            long batchEnd = Math.min(end, batchStart + 128);
            next = batchEnd;
            current = EdgePropertyCursorStreamingTest.batch(request, batchStart, batchEnd);
            return true;
        }

        @Override
        public RecordBatch batch() {
            return current;
        }

        @Override
        public void close() {
            closed = true;
            current = null;
        }

        private int totalBatchCount() {
            return (int) ((end - start + 127) / 128);
        }
    }

    private static RecordBatch batch(ReadRequest request, long start, long end) {
        List<Field> fields = new ArrayList<>();
        for (String name : request.projection().columns()) {
            fields.add(new Field(name, ColumnType.of(ColumnType.Kind.INT64), false));
        }
        Schema schema = new Schema(fields);
        return new RecordBatch() {
            @Override
            public Schema schema() {
                return schema;
            }

            @Override
            public int rowCount() {
                return Math.toIntExact(end - start);
            }

            @Override
            public Row row(int index) {
                long absolute = start + index;
                return column -> value(request.projection().columns().get(column), absolute);
            }
        };
    }

    private static Object value(String column, long row) {
        if ("_graphArSrcIndex".equals(column)) {
            return row % 50;
        }
        if ("_graphArDstIndex".equals(column)) {
            return row + 1;
        }
        if ("_graphArOffset".equals(column)) {
            return row == 1 ? 10L : 30L;
        }
        if ("weight".equals(column)) {
            return (double) row;
        }
        return "kind-" + row;
    }
}
