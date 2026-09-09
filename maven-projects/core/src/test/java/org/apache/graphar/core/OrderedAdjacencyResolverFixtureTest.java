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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.info.type.AdjListType;
import org.junit.Assume;
import org.junit.Test;

/** Verifies ordered adjacency resolution against the canonical LDBC GraphAr fixture. */
public class OrderedAdjacencyResolverFixtureTest {
    @Test
    public void resolvesLdbcOffsetPairAcrossTwoCanonicalAdjacencyChunks() throws Exception {
        Path fixtureRoot = canonicalFixtureRoot();
        Assume.assumeTrue(
                "The canonical GraphAr testing fixtures are unavailable."
                        + " Set GAR_TEST_DATA to the testing directory to run this test.",
                fixtureRoot != null);
        EdgeInfo edgeInfo =
                new LocalFileSystemStringGraphInfoLoader()
                        .loadEdgeInfo(fixtureRoot.resolve("person_knows_person.edge.yml").toUri());
        OrderedAdjacencyResolver resolver =
                new OrderedAdjacencyResolver(edgeInfo, AdjListType.ordered_by_source);
        OffsetChunk offsets = OffsetChunk.of(2, offsetsForCanonicalVertex297());

        assertEquals(100, offsets.vertexCount());
        offsets.validateEdgeCount(1077);

        ResolvedAdjacency resolved = resolver.resolve(297, offsets);

        assertEquals(
                new OffsetLocation(
                        297,
                        2,
                        97,
                        URI.create("edge/person_knows_person/ordered_by_source/offset/chunk2")),
                resolved.offsetLocation());
        assertEquals(EdgeRange.fromOffsets(1008, 1061), resolved.edgeRange());
        assertEquals(new ChunkRange(0, 2), resolved.edgeChunks());
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
        assertThrows(IllegalArgumentException.class, () -> OffsetChunk.of(0, new long[] {1, 1}));
        assertThrows(IllegalArgumentException.class, () -> OffsetChunk.of(0, new long[] {0, 2, 1}));
        assertThrows(IllegalArgumentException.class, () -> OffsetChunk.of(-1, new long[] {0, 1}));
    }

    @Test
    public void rejectsAnOffsetChunkReadFromAnotherVertexChunk() throws Exception {
        Path fixtureRoot = canonicalFixtureRoot();
        Assume.assumeTrue(
                "The canonical GraphAr testing fixtures are unavailable."
                        + " Set GAR_TEST_DATA to the testing directory to run this test.",
                fixtureRoot != null);
        EdgeInfo edgeInfo =
                new LocalFileSystemStringGraphInfoLoader()
                        .loadEdgeInfo(fixtureRoot.resolve("person_knows_person.edge.yml").toUri());
        OrderedAdjacencyResolver resolver =
                new OrderedAdjacencyResolver(edgeInfo, AdjListType.ordered_by_source);
        OffsetChunk wrongChunk = OffsetChunk.of(1, offsetsForCanonicalVertex297());

        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class, () -> resolver.resolve(297, wrongChunk));

        assertTrue(failure.getMessage(), failure.getMessage().contains("vertex chunk 2"));
    }

    @Test
    public void describesRangesAsValues() {
        assertEquals(new ChunkRange(0, 2), new ChunkRange(0, 2));
        assertEquals(new ChunkRange(0, 2).hashCode(), new ChunkRange(0, 2).hashCode());
        assertNotEquals(new ChunkRange(0, 2), new ChunkRange(0, 3));
        assertEquals(EdgeRange.fromOffsets(3, 7), EdgeRange.fromOffsets(3, 7));
        assertNotEquals(EdgeRange.fromOffsets(3, 7), EdgeRange.fromOffsets(3, 8));
        assertEquals("ChunkRange[0, 2)", new ChunkRange(0, 2).toString());
        assertEquals("EdgeRange[3, 7)", EdgeRange.fromOffsets(3, 7).toString());
    }

    /**
     * Returns the parquet LDBC fixture directory, or {@code null} when the canonical testing data
     * is unavailable. Resolution follows the same order as the metadata module tests: the {@code
     * GAR_TEST_DATA} environment variable, the {@code gar.test.data} system property, then the
     * testing directory of a full checkout.
     */
    private static Path canonicalFixtureRoot() {
        String configured = System.getenv("GAR_TEST_DATA");
        if (configured == null) {
            configured = System.getProperty("gar.test.data");
        }
        if (configured != null) {
            Path candidate = Path.of(configured, "ldbc_sample", "parquet");
            return isFixtureRoot(candidate) ? candidate : null;
        }
        for (String relative : new String[] {"../../testing", "../testing", "testing"}) {
            Path candidate = Path.of(relative, "ldbc_sample", "parquet");
            if (isFixtureRoot(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private static boolean isFixtureRoot(Path candidate) {
        return Files.isRegularFile(candidate.resolve("person_knows_person.edge.yml"));
    }

    private static long[] offsetsForCanonicalVertex297() {
        long[] offsets = new long[101];
        for (int index = 1; index < 97; index++) {
            offsets[index] = index * 10L;
        }
        offsets[97] = 1008;
        offsets[98] = 1061;
        offsets[99] = 1061;
        offsets[100] = 1077;
        return offsets;
    }
}
