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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

/**
 * Proves the projection does not depend on which build strategy the available heap selects.
 *
 * <p>Buffering the endpoints costs a second copy of the topology, so a build that cannot afford
 * that reads the topology twice instead. The two paths place entries differently, so the guarantee
 * that matters is that they cannot be told apart from the result.
 */
public class HeterogeneousCsrStrategyTest {
    private static final long VERTEX_COUNT = 903L;
    private static final long EDGE_COUNT = 6626L;

    @Test
    public void aBudgetTooSmallToBufferEndpointsBuildsTheSameProjection() throws Exception {
        for (CsrDirection direction : CsrDirection.values()) {
            long buffered = ProjectionCapacity.peakBuildBytes(VERTEX_COUNT, EDGE_COUNT, direction);
            long twoScan =
                    ProjectionCapacity.twoScanPeakBuildBytes(VERTEX_COUNT, EDGE_COUNT, direction);
            assertTrue("buffering endpoints must cost more than reading twice", twoScan < buffered);

            HeterogeneousCsr rich = build(direction, buffered);
            HeterogeneousCsr lean = build(direction, buffered - 1);

            assertEquals(direction.name(), rich.vertexCount(), lean.vertexCount());
            assertEquals(direction.name(), rich.edgeCount(), lean.edgeCount());
            assertEquals(direction.name(), rich.vertexTypes(), lean.vertexTypes());
            assertArrayEquals(
                    "offsets under " + direction, rich.csr().offsets(), lean.csr().offsets());
            assertArrayEquals(
                    "destinations under " + direction,
                    rich.csr().destinations(),
                    lean.csr().destinations());
            for (long vertex = 0; vertex < VERTEX_COUNT; vertex += 37L) {
                assertArrayEquals(
                        "neighbors of " + vertex + " under " + direction,
                        rich.neighbors(vertex),
                        lean.neighbors(vertex));
            }
        }
    }

    @Test
    public void anIdentifierResolvesToTheSameVertexWhicheverStrategyBuiltIt() throws Exception {
        HeterogeneousCsr rich = build(CsrDirection.UNDIRECTED, Long.MAX_VALUE / 4L);
        HeterogeneousCsr lean =
                build(
                        CsrDirection.UNDIRECTED,
                        ProjectionCapacity.peakBuildBytes(
                                        VERTEX_COUNT, EDGE_COUNT, CsrDirection.UNDIRECTED)
                                - 1);

        assertEquals(rich.base("person"), lean.base("person"));
        for (String nodeId : new String[] {"933", "1129", "4139", "absent"}) {
            assertEquals(
                    "global identifier of " + nodeId,
                    rich.globalIndex("person", nodeId),
                    lean.globalIndex("person", nodeId));
        }
    }

    @Test
    public void aBudgetTooSmallForEvenTheProjectionIsRefusedBeforeAllocating() throws Exception {
        long twoScan =
                ProjectionCapacity.twoScanPeakBuildBytes(
                        VERTEX_COUNT, EDGE_COUNT, CsrDirection.UNDIRECTED);
        try {
            build(CsrDirection.UNDIRECTED, twoScan - 1);
            throw new AssertionError("Expected the build to be refused.");
        } catch (ProjectionTooLargeException expected) {
            assertTrue(expected.getMessage().contains("bytes are available"));
        }
    }

    private static HeterogeneousCsr build(CsrDirection direction, long availableBytes)
            throws IOException {
        return HeterogeneousCsr.builder(openGraph())
                .addVertexType("person")
                .addEdgeType("person", "knows", "person")
                .direction(direction)
                .build(availableBytes);
    }

    private static GraphReader openGraph() throws IOException {
        return GraphReader.open(
                fixturePath().resolve("ldbc_sample.graph.yml").toUri(),
                new LocalFileSystemStringGraphInfoLoader(),
                new LocalStorage(),
                new ParquetPhysicalReader(new LocalStorage()));
    }

    private static Path fixturePath() {
        return Path.of("..", "..", "testing", "ldbc_sample", "parquet");
    }
}
