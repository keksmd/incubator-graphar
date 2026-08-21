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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import org.junit.Test;

/**
 * Proves a projection extended by a batch of edges is the projection a full rebuild over the same
 * edges would produce.
 *
 * <p>Merging is only worth having if it is indistinguishable from rebuilding, so every case here
 * compares against the rebuild rather than against a hand-written expectation.
 */
public class CsrMergeTest {
    private static final long SEED = 20260813L;

    @Test
    public void aMergedProjectionEqualsARebuildOverEveryEdge() throws Exception {
        for (CsrDirection direction : CsrDirection.values()) {
            int vertices = 5_000;
            int[] baseSources = new int[12_000];
            int[] baseTargets = new int[12_000];
            int[] deltaSources = new int[3_000];
            int[] deltaTargets = new int[3_000];
            Random random = new Random(SEED);
            fill(random, baseSources, baseTargets, vertices);
            fill(random, deltaSources, deltaTargets, vertices);

            CsrGraph base = build(baseSources, baseTargets, vertices, direction);
            CsrGraph merged =
                    CsrMaterializer.merge(
                            base,
                            deltaSources,
                            deltaTargets,
                            deltaSources.length,
                            vertices,
                            direction);
            CsrGraph rebuilt =
                    build(
                            concat(baseSources, deltaSources),
                            concat(baseTargets, deltaTargets),
                            vertices,
                            direction);

            assertEquals(direction.name(), rebuilt.vertexCount(), merged.vertexCount());
            assertEquals(direction.name(), rebuilt.edgeCount(), merged.edgeCount());
            assertArrayEquals(direction.name(), rebuilt.rawOffsets(), merged.rawOffsets());
            assertArrayEquals(
                    direction.name(), rebuilt.rawDestinations(), merged.rawDestinations());
        }
    }

    @Test
    public void verticesAppendedToTheDatasetEnterTheMergedProjection() throws Exception {
        int baseVertices = 800;
        int grownVertices = 1_200;
        int[] baseSources = new int[2_000];
        int[] baseTargets = new int[2_000];
        Random random = new Random(SEED);
        fill(random, baseSources, baseTargets, baseVertices);
        int[] deltaSources = new int[700];
        int[] deltaTargets = new int[700];
        for (int edge = 0; edge < deltaSources.length; edge++) {
            deltaSources[edge] = baseVertices + random.nextInt(grownVertices - baseVertices);
            deltaTargets[edge] = random.nextInt(grownVertices);
        }

        CsrGraph base = build(baseSources, baseTargets, baseVertices, CsrDirection.UNDIRECTED);
        CsrGraph merged =
                CsrMaterializer.merge(
                        base,
                        deltaSources,
                        deltaTargets,
                        deltaSources.length,
                        grownVertices,
                        CsrDirection.UNDIRECTED);
        CsrGraph rebuilt =
                build(
                        concat(baseSources, deltaSources),
                        concat(baseTargets, deltaTargets),
                        grownVertices,
                        CsrDirection.UNDIRECTED);

        assertEquals(grownVertices, merged.vertexCount());
        assertArrayEquals(rebuilt.rawOffsets(), merged.rawOffsets());
        assertArrayEquals(rebuilt.rawDestinations(), merged.rawDestinations());
    }

    @Test
    public void relocatedVerticesCarryTheirAdjacencyIntoTheGrownSpace() throws Exception {
        for (CsrDirection direction : CsrDirection.values()) {
            int firstCount = 900;
            int secondCount = 600;
            int firstArriving = 120;
            int secondArriving = 80;
            long[] bases = {0L, firstCount};
            long[] grownBases = {0L, firstCount + firstArriving};
            long[] counts = {firstCount, secondCount};
            long grownVertices = firstCount + firstArriving + secondCount + secondArriving;
            CsrMaterializer.VertexRelocation relocation = relocation(bases, counts, grownBases);

            Random random = new Random(SEED);
            int[] baseSources = new int[4_000];
            int[] baseTargets = new int[4_000];
            fill(random, baseSources, baseTargets, firstCount + secondCount);
            int[] deltaSources = new int[1_500];
            int[] deltaTargets = new int[1_500];
            fill(random, deltaSources, deltaTargets, Math.toIntExact(grownVertices));

            CsrGraph base = build(baseSources, baseTargets, firstCount + secondCount, direction);
            CsrGraph merged =
                    CsrMaterializer.merge(
                            base,
                            deltaSources,
                            deltaTargets,
                            deltaSources.length,
                            grownVertices,
                            direction,
                            relocation);
            CsrGraph rebuilt =
                    build(
                            concat(relocate(relocation, baseSources), deltaSources),
                            concat(relocate(relocation, baseTargets), deltaTargets),
                            grownVertices,
                            direction);

            assertEquals(direction.name(), grownVertices, merged.vertexCount());
            assertEquals(direction.name(), rebuilt.edgeCount(), merged.edgeCount());
            assertArrayEquals(direction.name(), rebuilt.rawOffsets(), merged.rawOffsets());
            assertArrayEquals(
                    direction.name(), rebuilt.rawDestinations(), merged.rawDestinations());
        }
    }

    @Test
    public void anEmptyBatchLeavesTheProjectionAsItWas() throws Exception {
        int[] sources = new int[500];
        int[] targets = new int[500];
        fill(new Random(SEED), sources, targets, 300);
        CsrGraph base = build(sources, targets, 300, CsrDirection.UNDIRECTED);

        CsrGraph merged =
                CsrMaterializer.merge(
                        base, new int[0], new int[0], 0, 300, CsrDirection.UNDIRECTED);

        assertArrayEquals(base.rawOffsets(), merged.rawOffsets());
        assertArrayEquals(base.rawDestinations(), merged.rawDestinations());
    }

    @Test
    public void theBaseProjectionIsUntouchedByAMerge() throws Exception {
        int[] sources = new int[900];
        int[] targets = new int[900];
        Random random = new Random(SEED);
        fill(random, sources, targets, 400);
        CsrGraph base = build(sources, targets, 400, CsrDirection.UNDIRECTED);
        long[] offsetsBefore = base.offsets();
        long[] destinationsBefore = base.destinations();

        int[] deltaSources = new int[200];
        int[] deltaTargets = new int[200];
        fill(random, deltaSources, deltaTargets, 400);
        CsrMaterializer.merge(base, deltaSources, deltaTargets, 200, 400, CsrDirection.UNDIRECTED);

        assertArrayEquals(offsetsBefore, base.offsets());
        assertArrayEquals(destinationsBefore, base.destinations());
    }

    @Test
    public void aBatchNamingAVertexOutsideTheProjectionIsRefused() throws Exception {
        CsrGraph base =
                CsrMaterializer.fromEndpoints(
                        new int[] {0, 1}, new int[] {1, 2}, 2, 3L, CsrDirection.UNDIRECTED);

        IllegalArgumentException refused =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                CsrMaterializer.merge(
                                        base,
                                        new int[] {7},
                                        new int[] {0},
                                        1,
                                        3L,
                                        CsrDirection.UNDIRECTED));
        assertTrue(refused.getMessage(), refused.getMessage().contains("outside the 3 vertices"));
    }

    @Test
    public void aMergeCannotShrinkTheVertexSpace() throws Exception {
        CsrGraph base =
                CsrMaterializer.fromEndpoints(
                        new int[] {0, 1}, new int[] {1, 2}, 2, 4L, CsrDirection.UNDIRECTED);

        IllegalArgumentException refused =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                CsrMaterializer.merge(
                                        base,
                                        new int[0],
                                        new int[0],
                                        0,
                                        2L,
                                        CsrDirection.UNDIRECTED));
        assertTrue(refused.getMessage(), refused.getMessage().contains("cannot lose vertices"));
    }

    /**
     * Returns the relocation two vertex types undergo when vertices arrive for both, which is what
     * a projection concatenating those types hands the merge.
     */
    private static CsrMaterializer.VertexRelocation relocation(
            long[] bases, long[] counts, long[] grownBases) {
        return new CsrMaterializer.VertexRelocation() {
            @Override
            public int relocate(int baseVertex) {
                int ordinal = baseVertex < bases[1] ? 0 : 1;
                return Math.toIntExact(grownBases[ordinal] + (baseVertex - bases[ordinal]));
            }

            @Override
            public int origin(int mergedVertex) {
                int ordinal = mergedVertex < grownBases[1] ? 0 : 1;
                long local = mergedVertex - grownBases[ordinal];
                return local < counts[ordinal]
                        ? Math.toIntExact(bases[ordinal] + local)
                        : CsrMaterializer.VertexRelocation.ABSENT;
            }
        };
    }

    private static int[] relocate(CsrMaterializer.VertexRelocation relocation, int[] vertices) {
        int[] relocated = new int[vertices.length];
        for (int position = 0; position < vertices.length; position++) {
            relocated[position] = relocation.relocate(vertices[position]);
        }
        return relocated;
    }

    private static void fill(Random random, int[] sources, int[] targets, int vertices) {
        for (int edge = 0; edge < sources.length; edge++) {
            sources[edge] = random.nextInt(vertices);
            targets[edge] = random.nextInt(vertices);
        }
    }

    private static int[] concat(int[] first, int[] second) {
        int[] joined = new int[first.length + second.length];
        System.arraycopy(first, 0, joined, 0, first.length);
        System.arraycopy(second, 0, joined, first.length, second.length);
        return joined;
    }

    private static CsrGraph build(
            int[] sources, int[] targets, long vertices, CsrDirection direction)
            throws IOException {
        return CsrMaterializer.fromEndpoints(
                sources.clone(), targets.clone(), sources.length, vertices, direction);
    }
}
