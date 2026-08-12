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

package org.apache.graphar.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

public class OrderedAdjacencyResolverFixtureTest {
    @Test
    public void resolvesLdbcOffsetPairAcrossTwoCanonicalAdjacencyChunks() throws Exception {
        Path fixtureRoot = Path.of("..", "..", "testing", "ldbc_sample", "parquet");
        EdgeInfo edgeInfo =
                new LocalFileSystemStringGraphInfoLoader()
                        .loadEdgeInfo(fixtureRoot.resolve("person_knows_person.edge.yml").toUri());
        OrderedAdjacencyResolver resolver =
                new OrderedAdjacencyResolver(edgeInfo, AdjListType.ordered_by_source);
        OffsetChunk offsets =
                OffsetChunk.of(
                        readLongColumn(
                                fixtureRoot.resolve(
                                        "edge/person_knows_person/ordered_by_source/offset/chunk2")));

        assertEquals(100, offsets.vertexCount());
        long edgeCount =
                readLittleEndianInt64(
                        fixtureRoot.resolve(
                                "edge/person_knows_person/ordered_by_source/edge_count2"));
        assertEquals(1077, edgeCount);
        offsets.validateEdgeCount(edgeCount);

        ResolvedAdjacency resolved = resolver.resolve(297, offsets);

        assertEquals(2, resolved.offsetLocation().vertexChunkIndex());
        assertEquals(97, resolved.offsetLocation().offsetIndex());
        assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/offset/chunk2"),
                resolved.offsetLocation().offsetChunkUri());
        assertEquals(1008, resolved.edgeRange().begin());
        assertEquals(1061, resolved.edgeRange().end());
        assertEquals(0, resolved.edgeChunks().begin());
        assertEquals(2, resolved.edgeChunks().end());
        assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/part2/chunk0"),
                resolved.adjacencyChunkUri(0));
        assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/part2/chunk1"),
                resolved.adjacencyChunkUri(1));
        assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/edge_count2"),
                resolved.edgeCountUri());
        assertFalse(resolved.edgeChunks().isEmpty());
        assertTrue(
                Files.isRegularFile(
                        fixtureRoot.resolve(
                                "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk0")));
        assertTrue(
                Files.isRegularFile(
                        fixtureRoot.resolve(
                                "edge/person_knows_person/ordered_by_source/adj_list/part2/chunk1")));
    }

    @Test
    public void preservesLongAndHalfOpenBoundaries() {
        assertEquals(2, ChunkMath.chunkIndex(299, 100));
        assertEquals(99, ChunkMath.offsetInChunk(299, 100));
        assertEquals(3, ChunkMath.chunkCount(201, 100));
        assertEquals(Long.MAX_VALUE, ChunkMath.chunkCount(Long.MAX_VALUE, 1));

        EdgeRange range = EdgeRange.fromOffsets(1024, 1025);
        assertEquals(1, range.edgeChunks(1024).begin());
        assertEquals(2, range.edgeChunks(1024).end());
        assertFalse(range.edgeChunks(1024).isEmpty());
        assertEquals(1, EdgeRange.fromOffsets(1024, 1024).edgeChunks(1024).begin());
        assertEquals(1, EdgeRange.fromOffsets(1024, 1024).edgeChunks(1024).end());

        assertThrows(IllegalArgumentException.class, () -> ChunkMath.chunkIndex(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> EdgeRange.fromOffsets(5, 4));
        assertThrows(IllegalArgumentException.class, () -> OffsetChunk.of(new long[] {1, 1}));
        assertThrows(IllegalArgumentException.class, () -> OffsetChunk.of(new long[] {0, 2, 1}));
    }

    private static long[] readLongColumn(Path path) throws IOException {
        ReadResult result =
                new ParquetPhysicalReader(new LocalStorage())
                        .read(ReadRequest.builder(path.toUri()).build());
        List<Long> values = new ArrayList<>();
        try (BatchCursor cursor = result.cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                for (int index = 0; index < batch.rowCount(); index++) {
                    values.add((Long) batch.row(index).value(0));
                }
            }
        }
        long[] offsets = new long[values.size()];
        for (int index = 0; index < offsets.length; index++) {
            offsets[index] = values.get(index);
        }
        return offsets;
    }

    private static long readLittleEndianInt64(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        if (bytes.length != Long.BYTES) {
            throw new IllegalArgumentException("Expected one INT64 value in " + path + ".");
        }
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
}
