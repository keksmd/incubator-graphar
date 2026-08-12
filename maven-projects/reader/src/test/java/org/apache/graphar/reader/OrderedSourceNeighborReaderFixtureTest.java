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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadCapability;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

public class OrderedSourceNeighborReaderFixtureTest {
    @Test
    public void readsNeighborsAcrossAdjacencyChunksWithProjectionAndRanges() throws Exception {
        CapturingPhysicalReader physicalReader = new CapturingPhysicalReader(parquetReader());
        OrderedSourceNeighborReader reader =
                new OrderedSourceNeighborReader(loadEdgeInfo(), fixtureRoot(), physicalReader);

        List<Long> neighbors;
        List<org.apache.graphar.io.ReadReport> reports;
        try (NeighborCursor cursor = reader.neighbors(297)) {
            neighbors = destinations(cursor);
            reports = cursor.reports();
        }

        assertEquals(53, neighbors.size());
        assertEquals(
                List.of(
                        4L, 25L, 28L, 45L, 58L, 62L, 74L, 84L, 104L, 105L, 126L, 130L, 169L, 180L,
                        197L, 201L, 231L, 252L, 262L, 271L, 273L, 300L, 307L, 324L, 345L, 357L,
                        385L, 425L, 468L, 470L, 507L, 538L, 540L, 544L, 550L, 566L, 576L, 587L,
                        604L, 614L, 622L, 623L, 652L, 671L, 678L, 698L, 749L, 756L, 777L, 840L,
                        851L, 878L, 884L),
                neighbors);
        assertEquals(3, physicalReader.requests.size());
        assertRequest(
                physicalReader.requests.get(0),
                "edge/person_knows_person/ordered_by_source/offset/chunk2",
                "_graphArOffset",
                new RowRange(97, 99),
                null);
        assertRequest(
                physicalReader.requests.get(1),
                "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk0",
                "_graphArDstIndex",
                new RowRange(1008, 1024),
                null);
        assertRequest(
                physicalReader.requests.get(2),
                "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk1",
                "_graphArDstIndex",
                new RowRange(0, 37),
                null);
        assertEquals(3, reports.size());
        for (org.apache.graphar.io.ReadReport report : reports) {
            assertTrue(report.applied().contains(ReadCapability.PROJECTION));
            assertTrue(report.declined().contains(ReadCapability.ROW_RANGE));
        }
    }

    @Test
    public void pushesLimitWithoutOpeningUnneededEdgeChunks() throws Exception {
        CapturingPhysicalReader unlimitedPhysicalReader =
                new CapturingPhysicalReader(parquetReader());
        OrderedSourceNeighborReader unlimitedReader =
                new OrderedSourceNeighborReader(
                        loadEdgeInfo(), fixtureRoot(), unlimitedPhysicalReader);
        List<Long> allNeighbors;
        try (NeighborCursor cursor = unlimitedReader.neighbors(297)) {
            allNeighbors = destinations(cursor);
        }

        CapturingPhysicalReader limitedPhysicalReader =
                new CapturingPhysicalReader(parquetReader());
        OrderedSourceNeighborReader limitedReader =
                new OrderedSourceNeighborReader(
                        loadEdgeInfo(), fixtureRoot(), limitedPhysicalReader);
        List<Long> limitedNeighbors;
        try (NeighborCursor cursor = limitedReader.neighbors(297, 2)) {
            limitedNeighbors = destinations(cursor);
            assertEquals(2, cursor.reports().size());
        }

        assertEquals(allNeighbors.subList(0, 2), limitedNeighbors);
        assertEquals(2, limitedPhysicalReader.requests.size());
        assertRequest(
                limitedPhysicalReader.requests.get(1),
                "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk0",
                "_graphArDstIndex",
                new RowRange(1008, 1024),
                2L);
    }

    @Test
    public void doesNotOpenAnAdjacencyFileForAnEmptyOffsetRange() throws Exception {
        CapturingPhysicalReader physicalReader = new CapturingPhysicalReader(parquetReader());
        OrderedSourceNeighborReader reader =
                new OrderedSourceNeighborReader(loadEdgeInfo(), fixtureRoot(), physicalReader);

        try (NeighborCursor cursor = reader.neighbors(200)) {
            assertFalse(cursor.next());
            assertEquals(1, cursor.reports().size());
        }

        assertEquals(1, physicalReader.requests.size());
        assertRequest(
                physicalReader.requests.get(0),
                "edge/person_knows_person/ordered_by_source/offset/chunk2",
                "_graphArOffset",
                new RowRange(0, 2),
                null);
    }

    private static EdgeInfo loadEdgeInfo() throws IOException {
        Path fixtureRoot = fixturePath();
        return new LocalFileSystemStringGraphInfoLoader()
                .loadEdgeInfo(fixtureRoot.resolve("person_knows_person.edge.yml").toUri());
    }

    private static URI fixtureRoot() {
        return fixturePath().toUri();
    }

    private static Path fixturePath() {
        return Path.of("..", "..", "testing", "ldbc_sample", "parquet");
    }

    private static PhysicalReader parquetReader() {
        return new ParquetPhysicalReader(new LocalStorage());
    }

    private static List<Long> destinations(NeighborCursor cursor) throws IOException {
        List<Long> values = new ArrayList<>();
        while (cursor.next()) {
            values.add(cursor.destination());
        }
        return values;
    }

    private static void assertRequest(
            ReadRequest request,
            String expectedUri,
            String expectedColumn,
            RowRange expectedRange,
            Long limit) {
        assertEquals(fixtureRoot().resolve(expectedUri), request.uri());
        assertEquals(List.of(expectedColumn), request.projection().columns());
        assertEquals(expectedRange, request.rowRange().orElseThrow());
        if (limit == null) {
            assertFalse(request.limit().isPresent());
        } else {
            assertTrue(request.limit().isPresent());
            assertEquals(limit.longValue(), request.limit().getAsLong());
        }
    }

    private static final class CapturingPhysicalReader implements PhysicalReader {
        private final PhysicalReader delegate;
        private final List<ReadRequest> requests = new ArrayList<>();

        private CapturingPhysicalReader(PhysicalReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public java.util.Set<ReadCapability> capabilities() {
            return delegate.capabilities();
        }

        @Override
        public ReadResult read(ReadRequest request) throws IOException {
            requests.add(request);
            return delegate.read(request);
        }
    }
}
