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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

public class VertexReaderFixtureTest {
    @Test
    public void readsDeclaredPropertiesOfOneVertexFromItsChunkOnly() throws Exception {
        CapturingPhysicalReader physicalReader = new CapturingPhysicalReader(parquetReader());
        VertexReader vertices = openGraph(physicalReader).vertex("person");

        assertEquals(903L, vertices.vertexCount());
        assertEquals(
                Map.of(
                        "id",
                        13194139533574L,
                        "firstName",
                        "Benhalima",
                        "lastName",
                        "Ferrer",
                        "gender",
                        "male"),
                vertices.properties(297));

        assertEquals(2, physicalReader.requests.size());
        assertRequest(
                physicalReader.requests.get(0),
                "vertex/person/id/chunk2",
                List.of("_graphArVertexIndex", "id"),
                new RowRange(97, 98));
        assertRequest(
                physicalReader.requests.get(1),
                "vertex/person/firstName_lastName_gender/chunk2",
                List.of("_graphArVertexIndex", "firstName", "lastName", "gender"),
                new RowRange(97, 98));
    }

    @Test
    public void readsOnlyTheProjectedPropertyGroup() throws Exception {
        CapturingPhysicalReader physicalReader = new CapturingPhysicalReader(parquetReader());
        VertexReader vertices = openGraph(physicalReader).vertex("person");

        assertEquals(Map.of("id", 933L), vertices.properties(0, List.of("id")));

        assertEquals(1, physicalReader.requests.size());
        assertRequest(
                physicalReader.requests.get(0),
                "vertex/person/id/chunk0",
                List.of("_graphArVertexIndex", "id"),
                new RowRange(0, 1));
    }

    @Test
    public void batchesConsecutiveVerticesIntoOneRangePerChunk() throws Exception {
        CapturingPhysicalReader physicalReader = new CapturingPhysicalReader(parquetReader());
        VertexReader vertices = openGraph(physicalReader).vertex("person");

        Map<Long, Map<String, Object>> read =
                vertices.properties(List.of(902L, 1L, 0L, 297L), List.of("id"));

        assertEquals(List.of(0L, 1L, 297L, 902L), new ArrayList<>(read.keySet()));
        assertEquals(933L, read.get(0L).get("id"));
        assertEquals(6597069767117L, read.get(1L).get("id"));
        assertEquals(13194139533574L, read.get(297L).get("id"));
        assertEquals(32985348834100L, read.get(902L).get("id"));

        assertEquals(3, physicalReader.requests.size());
        assertRequest(
                physicalReader.requests.get(0),
                "vertex/person/id/chunk0",
                List.of("_graphArVertexIndex", "id"),
                new RowRange(0, 2));
        assertRequest(
                physicalReader.requests.get(1),
                "vertex/person/id/chunk2",
                List.of("_graphArVertexIndex", "id"),
                new RowRange(97, 98));
        assertRequest(
                physicalReader.requests.get(2),
                "vertex/person/id/chunk9",
                List.of("_graphArVertexIndex", "id"),
                new RowRange(2, 3));
    }

    @Test
    public void scansEveryVertexInIndexOrder() throws Exception {
        VertexReader vertices =
                openGraph(new CapturingPhysicalReader(parquetReader())).vertex("person");

        long scanned = 0;
        long previous = -1;
        try (VertexPropertyCursor cursor = vertices.scan(List.of("id"))) {
            while (cursor.next()) {
                GraphVertex vertex = cursor.vertex();
                assertEquals(previous + 1, vertex.id());
                previous = vertex.id();
                assertTrue(vertex.property("id") instanceof Long);
                scanned++;
            }
            assertEquals(10, cursor.reports().size());
        }

        assertEquals(903L, scanned);
        assertEquals(902L, previous);
    }

    @Test
    public void limitsScanBeforeOpeningUnneededVertexChunks() throws Exception {
        CapturingPhysicalReader physicalReader = new CapturingPhysicalReader(parquetReader());
        VertexReader vertices = openGraph(physicalReader).vertex("person");

        List<Long> ids = new ArrayList<>();
        try (VertexPropertyCursor cursor = vertices.scan(List.of("id"), 2)) {
            while (cursor.next()) {
                ids.add(cursor.vertex().id());
            }
        }

        assertEquals(List.of(0L, 1L), ids);
        assertEquals(1, physicalReader.requests.size());
        assertRequest(
                physicalReader.requests.get(0),
                "vertex/person/id/chunk0",
                List.of("_graphArVertexIndex", "id"),
                new RowRange(0, 2));
    }

    @Test
    public void rejectsVerticesOutsideTheDeclaredCount() throws Exception {
        VertexReader vertices =
                openGraph(new CapturingPhysicalReader(parquetReader())).vertex("person");

        try {
            vertices.properties(903);
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("outside this vertex type"));
            return;
        }
        throw new AssertionError("Expected the declared vertex count to be enforced.");
    }

    @Test
    public void rejectsUndeclaredProperties() throws Exception {
        VertexReader vertices =
                openGraph(new CapturingPhysicalReader(parquetReader())).vertex("person");

        try {
            vertices.properties(0, List.of("missing"));
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("does not declare properties"));
            return;
        }
        throw new AssertionError("Expected undeclared vertex properties to be rejected.");
    }

    private static GraphReader openGraph(PhysicalReader physicalReader) throws IOException {
        return GraphReader.open(
                fixturePath().resolve("ldbc_sample.graph.yml").toUri(),
                new LocalFileSystemStringGraphInfoLoader(),
                new LocalStorage(),
                physicalReader);
    }

    private static void assertRequest(
            ReadRequest request,
            String expectedUri,
            List<String> expectedColumns,
            RowRange expectedRange) {
        assertEquals(fixturePath().toUri().resolve(expectedUri), request.uri());
        assertEquals(expectedColumns, request.projection().columns());
        assertEquals(expectedRange, request.rowRange().orElseThrow());
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
