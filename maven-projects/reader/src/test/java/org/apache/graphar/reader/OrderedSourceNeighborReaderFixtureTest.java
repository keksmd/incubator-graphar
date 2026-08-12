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
import static org.junit.Assert.assertThrows;
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
    public void rejectsPartialNeighborsWhenTheLegacyAdjacencyHasNoOffsetIndex() throws Exception {
        CapturingPhysicalReader physicalReader = new CapturingPhysicalReader(parquetReader());
        OrderedSourceNeighborReader reader =
                new OrderedSourceNeighborReader(loadEdgeInfo(), fixtureRoot(), physicalReader);

        try (NeighborCursor cursor = reader.neighbors(297)) {
            UnsupportedOperationException error =
                    assertThrows(UnsupportedOperationException.class, cursor::next);
            assertEquals(
                    "Physical Parquet row ranges require an Offset Index; refusing JVM fallback.",
                    error.getMessage());
        }

        assertEquals(2, physicalReader.requests.size());
        assertRequest(
                physicalReader.requests.get(0),
                "edge/person_knows_person/ordered_by_source/offset/chunk2",
                "_graphArOffset",
                null,
                null);
        assertRequest(
                physicalReader.requests.get(1),
                "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk0",
                "_graphArDstIndex",
                new RowRange(1008, 1024),
                null);
    }

    @Test
    public void cachesCompleteOffsetChunksInMemory() throws Exception {
        CapturingPhysicalReader physicalReader = new CapturingPhysicalReader(parquetReader());
        OrderedSourceNeighborReader reader =
                new OrderedSourceNeighborReader(loadEdgeInfo(), fixtureRoot(), physicalReader);

        try (NeighborCursor first = reader.neighbors(297);
                NeighborCursor second = reader.neighbors(298)) {
            assertEquals(1, first.reports().size());
            assertTrue(second.reports().isEmpty());
        }

        assertEquals(1, physicalReader.requests.size());
        assertRequest(
                physicalReader.requests.get(0),
                "edge/person_knows_person/ordered_by_source/offset/chunk2",
                "_graphArOffset",
                null,
                null);
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
                null,
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
        if (expectedRange == null) {
            assertFalse(request.rowRange().isPresent());
        } else {
            assertEquals(expectedRange, request.rowRange().orElseThrow());
        }
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
