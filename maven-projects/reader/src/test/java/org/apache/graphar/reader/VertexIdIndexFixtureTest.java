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
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

public class VertexIdIndexFixtureTest {
    @Test
    public void resolvesEveryDeclaredIdentifierToItsDenseVertexIndex() throws Exception {
        VertexReader vertices = openGraph(parquetReader()).vertex("person");
        VertexIdIndex index = VertexIdIndex.build(vertices);

        assertEquals("id", index.idProperty());
        assertEquals(903, index.size());
        assertEquals(903L, vertices.vertexCount());

        List<Long> everyVertex = new ArrayList<>();
        for (long vertexId = 0; vertexId < 903L; vertexId++) {
            everyVertex.add(vertexId);
        }
        int checked = 0;
        for (var entry : vertices.properties(everyVertex, List.of("id")).entrySet()) {
            String externalId = String.valueOf(entry.getValue().get("id"));
            assertEquals(
                    "identifier " + externalId,
                    entry.getKey().longValue(),
                    index.lookup(externalId));
            checked++;
        }
        assertEquals(903, checked);
    }

    @Test
    public void resolvesTheBenchmarkVertexAndFeedsItsNeighborLookup() throws Exception {
        GraphReader graph = openGraph(parquetReader());
        VertexIdIndex index = VertexIdIndex.build(graph.vertex("person"));

        long resolved = index.lookup("13194139533574");
        assertEquals(297L, resolved);

        List<Long> neighbors = new ArrayList<>();
        try (EdgeCursor cursor = graph.edge("person", "knows", "person").scanEdges()) {
            while (cursor.next()) {
                if (cursor.source() == resolved) {
                    neighbors.add(cursor.destination());
                }
            }
        }
        assertEquals(53, neighbors.size());
    }

    @Test
    public void readsOnlyTheIdentifierColumnWhileBuilding() throws Exception {
        CapturingPhysicalReader physicalReader = new CapturingPhysicalReader(parquetReader());
        VertexIdIndex.build(openGraph(physicalReader).vertex("person"));

        assertEquals(10, physicalReader.requests.size());
        for (ReadRequest request : physicalReader.requests) {
            assertTrue(
                    "unexpected chunk " + request.uri(),
                    request.uri().toString().contains("/vertex/person/id/chunk"));
            assertEquals(List.of("_graphArVertexIndex", "id"), request.projection().columns());
        }
    }

    @Test
    public void reportsUnknownIdentifiersAsAbsent() throws Exception {
        VertexIdIndex index = VertexIdIndex.build(openGraph(parquetReader()).vertex("person"));

        assertEquals(VertexIdIndex.ABSENT, index.lookup("no-such-identifier"));
        assertFalse(index.contains("no-such-identifier"));
        assertTrue(index.contains("13194139533574"));
    }

    @Test
    public void rejectsAnUndeclaredIdentifierProperty() throws Exception {
        VertexReader vertices = openGraph(parquetReader()).vertex("person");
        try {
            VertexIdIndex.build(vertices, "missing");
            throw new AssertionError("Expected an undeclared property to be rejected.");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("missing"));
        }
    }

    private static GraphReader openGraph(PhysicalReader physicalReader) throws IOException {
        return GraphReader.open(
                fixturePath().resolve("ldbc_sample.graph.yml").toUri(),
                new LocalFileSystemStringGraphInfoLoader(),
                new LocalStorage(),
                physicalReader);
    }

    private static PhysicalReader parquetReader() {
        return new ParquetPhysicalReader(new LocalStorage());
    }

    private static Path fixturePath() {
        return Path.of("..", "..", "testing", "ldbc_sample", "parquet");
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
