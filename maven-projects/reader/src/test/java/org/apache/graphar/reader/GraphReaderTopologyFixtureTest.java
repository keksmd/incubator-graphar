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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

public class GraphReaderTopologyFixtureTest {
    @Test
    public void opensGraphScansTopologyAndMaterializesCsr() throws Exception {
        CapturingPhysicalReader physicalReader = new CapturingPhysicalReader(parquetReader());
        GraphReader graph = openGraph(physicalReader);
        OrderedSourceEdgeReader edges = graph.edge("person", "knows", "person");

        assertEquals("ldbc_sample", graph.graphInfo().getName());
        assertEquals(903L, edges.vertexCount());
        assertEquals(6626L, edges.edgeCount());

        List<Long> source297 = new ArrayList<>();
        long previousSource = -1;
        long scanned = 0;
        try (EdgeCursor cursor = edges.scanEdges()) {
            while (cursor.next()) {
                assertTrue(cursor.source() >= previousSource);
                previousSource = cursor.source();
                if (cursor.source() == 297) {
                    source297.add(cursor.destination());
                }
                scanned++;
            }
            assertEquals(11, cursor.reports().size());
        }

        assertEquals(6626L, scanned);
        assertEquals(expectedNeighbors297(), source297);
        assertEquals(11, physicalReader.requests.size());
        assertTopologyRequest(
                physicalReader.requests.get(2),
                "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk0",
                new RowRange(0, 1024),
                null);
        assertTopologyRequest(
                physicalReader.requests.get(3),
                "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk1",
                new RowRange(0, 53),
                null);

        CsrGraph csr = edges.materializeCsr(903, 6626);
        assertEquals(903L, csr.vertexCount());
        assertEquals(6626L, csr.edgeCount());
        long[] offsets = csr.offsets();
        long[] destinations = csr.destinations();
        assertEquals(2319L, offsets[297]);
        assertEquals(2372L, offsets[298]);
        assertEquals(expectedNeighbors297(), slice(destinations, offsets[297], offsets[298]));
    }

    @Test
    public void limitsScanBeforeOpeningUnneededTopologyChunks() throws Exception {
        CapturingPhysicalReader physicalReader = new CapturingPhysicalReader(parquetReader());
        OrderedSourceEdgeReader edges = openGraph(physicalReader).edge("person", "knows", "person");

        long count = 0;
        try (EdgeCursor cursor = edges.scanEdges(2)) {
            while (cursor.next()) {
                count++;
            }
            assertEquals(1, cursor.reports().size());
        }

        assertEquals(2L, count);
        assertEquals(1, physicalReader.requests.size());
        assertTopologyRequest(
                physicalReader.requests.get(0),
                "edge/person_knows_person/ordered_by_source/adj_list/part0/chunk0",
                new RowRange(0, 667),
                2L);
    }

    @Test
    public void rejectsCsrBoundsBeforeAllocatingArrays() throws Exception {
        OrderedSourceEdgeReader edges =
                openGraph(new CapturingPhysicalReader(parquetReader()))
                        .edge("person", "knows", "person");

        try {
            edges.materializeCsr(902, 6626);
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("configured CSR bounds"));
            return;
        }
        throw new AssertionError("Expected configured CSR bounds to be enforced.");
    }

    private static GraphReader openGraph(PhysicalReader physicalReader) throws IOException {
        return GraphReader.open(
                fixturePath().resolve("ldbc_sample.graph.yml").toUri(),
                new LocalFileSystemStringGraphInfoLoader(),
                new LocalStorage(),
                physicalReader);
    }

    private static List<Long> slice(long[] values, long begin, long end) {
        List<Long> result = new ArrayList<>();
        for (long index = begin; index < end; index++) {
            result.add(values[(int) index]);
        }
        return result;
    }

    private static List<Long> expectedNeighbors297() {
        return List.of(
                4L, 25L, 28L, 45L, 58L, 62L, 74L, 84L, 104L, 105L, 126L, 130L, 169L, 180L, 197L,
                201L, 231L, 252L, 262L, 271L, 273L, 300L, 307L, 324L, 345L, 357L, 385L, 425L, 468L,
                470L, 507L, 538L, 540L, 544L, 550L, 566L, 576L, 587L, 604L, 614L, 622L, 623L, 652L,
                671L, 678L, 698L, 749L, 756L, 777L, 840L, 851L, 878L, 884L);
    }

    private static void assertTopologyRequest(
            ReadRequest request, String expectedUri, RowRange expectedRange, Long limit) {
        assertEquals(fixturePath().toUri().resolve(expectedUri), request.uri());
        assertEquals(
                List.of("_graphArSrcIndex", "_graphArDstIndex"), request.projection().columns());
        assertEquals(expectedRange, request.rowRange().orElseThrow());
        if (limit == null) {
            assertFalse(request.limit().isPresent());
        } else {
            assertTrue(request.limit().isPresent());
            assertEquals(limit.longValue(), request.limit().getAsLong());
        }
    }

    private static Path fixturePath() {
        return Path.of("..", "..", "testing", "ldbc_sample", "parquet");
    }

    private static PhysicalReader parquetReader() {
        return new ParquetPhysicalReader(new LocalStorage());
    }

    private static final class CapturingPhysicalReader implements PhysicalReader {
        private final PhysicalReader delegate;
        private final List<ReadRequest> requests = new ArrayList<>();

        private CapturingPhysicalReader(PhysicalReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public java.util.Set<org.apache.graphar.io.ReadCapability> capabilities() {
            return delegate.capabilities();
        }

        @Override
        public ReadResult read(ReadRequest request) throws IOException {
            requests.add(request);
            return delegate.read(request);
        }
    }
}
