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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

/**
 * Proves edges that arrive after a projection was built can be served without reading the dataset
 * again, and that publishing the extended projection follows the rule a rebuild follows.
 */
public class GraphProjectionMergeTest {
    private static final String PERSON_ID = "13194139533574";
    private static final String ARRIVING_ID = "99000000000001";
    private static final String SECOND_ARRIVING_ID = "99000000000002";

    /** Returns a vertex the fixture does not already put next to {@code vertex}. */
    private static long unrelated(HeterogeneousCsr projection, long vertex) {
        for (long candidate = 0; candidate < projection.vertexCount(); candidate++) {
            if (candidate != vertex && !contains(projection.neighbors(vertex), candidate)) {
                return candidate;
            }
        }
        throw new AssertionError("Every vertex is already adjacent to " + vertex);
    }

    @Test
    public void aMergedProjectionServesTheEdgesThatArrivedAfterItWasBuilt() throws Exception {
        HeterogeneousCsr projection = build();
        long first = projection.globalIndex("person", PERSON_ID);
        assertTrue("the fixture must resolve the identifier", first >= 0);
        long second = unrelated(projection, first);
        long[] before = projection.neighbors(first);

        HeterogeneousCsr merged = projection.merge(new long[] {first}, new long[] {second}, 1);

        assertTrue(contains(merged.neighbors(first), second));
        assertTrue(contains(merged.neighbors(second), first));
        assertEquals(projection.edgeCount() + 2L, merged.edgeCount());
        assertEquals(projection.vertexCount(), merged.vertexCount());
        assertArrayEquals(
                "the projection being served must not change under a merge",
                before,
                projection.neighbors(first));
        long[] adjacency = merged.neighbors(first);
        long[] sorted = adjacency.clone();
        Arrays.sort(sorted);
        assertArrayEquals("a merged adjacency stays ordered", sorted, adjacency);
    }

    @Test
    public void publishingAMergedProjectionAdvancesTheServedGeneration() throws Exception {
        GraphProjection served = GraphProjection.load(GraphProjectionMergeTest::build);
        GraphProjection.Snapshot initial = served.current();
        long first = initial.projection().globalIndex("person", PERSON_ID);
        long second = unrelated(initial.projection(), first);

        GraphProjection.Snapshot published =
                served.publish(
                        initial.projection().merge(new long[] {first}, new long[] {second}, 1));

        assertEquals(initial.generation() + 1L, published.generation());
        assertEquals(published, served.current());
        assertTrue(contains(served.current().projection().neighbors(first), second));
        assertFalse(
                "a snapshot already handed out keeps answering from the graph it was built on",
                contains(initial.projection().neighbors(first), second));
    }

    @Test
    public void aMergedProjectionLearnsTheVerticesTheBatchIntroduces() throws Exception {
        HeterogeneousCsr projection = build();
        long known = projection.globalIndex("person", PERSON_ID);

        HeterogeneousCsr merged = projection.merge(arrivingBatch());

        assertEquals(projection.vertexCount() + 2L, merged.vertexCount());
        assertEquals(projection.edgeCount() + 4L, merged.edgeCount());
        long arrived = merged.globalIndex("person", ARRIVING_ID);
        assertEquals(projection.vertexCount(), arrived);
        assertEquals(
                "the identifier index being served must not learn the arriving vertex",
                VertexIdIndex.ABSENT,
                projection.globalIndex("person", ARRIVING_ID));
        assertTrue(contains(merged.neighbors(known), arrived));
        assertTrue(contains(merged.neighbors(arrived), known));
        long[] adjacency = merged.neighbors(known);
        long[] sorted = adjacency.clone();
        Arrays.sort(sorted);
        assertArrayEquals("a merged adjacency stays ordered", sorted, adjacency);
    }

    @Test
    public void mergingABatchIntoTheServedProjectionAdvancesTheGeneration() throws Exception {
        GraphProjection served = GraphProjection.load(GraphProjectionMergeTest::build);
        GraphProjection.Snapshot initial = served.current();
        long known = initial.projection().globalIndex("person", PERSON_ID);

        GraphProjection.Snapshot published = served.merge(arrivingBatch());

        assertEquals(initial.generation() + 1L, published.generation());
        assertEquals(published, served.current());
        long arrived = published.projection().globalIndex("person", ARRIVING_ID);
        assertTrue(contains(served.current().projection().neighbors(known), arrived));
        assertEquals(
                "a snapshot already handed out keeps answering from the graph it was built on",
                VertexIdIndex.ABSENT,
                initial.projection().globalIndex("person", ARRIVING_ID));
        assertEquals(
                initial.projection().vertexCount() + 2L,
                served.current().projection().vertexCount());
    }

    @Test
    public void mergingWhileARebuildIsInFlightIsRefused() throws Exception {
        CountDownLatch building = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger builds = new AtomicInteger();
        GraphProjection served =
                GraphProjection.load(
                        () -> {
                            if (builds.getAndIncrement() > 0) {
                                building.countDown();
                                try {
                                    release.await();
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    throw new IOException(interrupted);
                                }
                            }
                            return build();
                        });
        GraphProjection.Snapshot initial = served.current();
        AtomicReference<Exception> rebuildFailure = new AtomicReference<>();
        Thread rebuild =
                new Thread(
                        () -> {
                            try {
                                served.refresh();
                            } catch (Exception failure) {
                                rebuildFailure.set(failure);
                            }
                        });
        rebuild.start();
        building.await();

        try {
            IllegalStateException refused =
                    assertThrows(IllegalStateException.class, () -> served.merge(arrivingBatch()));
            assertTrue(refused.getMessage(), refused.getMessage().contains("rebuild is in flight"));
            assertEquals(initial, served.current());
        } finally {
            release.countDown();
            rebuild.join();
        }

        assertNull(rebuildFailure.get());
        assertEquals(initial.generation() + 1L, served.current().generation());
    }

    /**
     * Returns a batch that names two identifiers the dataset does not hold, which is what an
     * identity graph receives when a person is seen for the first time.
     */
    private static HeterogeneousCsr.MergeBatch arrivingBatch() {
        return HeterogeneousCsr.batch()
                .addEdge("person", ARRIVING_ID, "person", PERSON_ID)
                .addEdge("person", ARRIVING_ID, "person", SECOND_ARRIVING_ID);
    }

    private static boolean contains(long[] values, long wanted) {
        for (long value : values) {
            if (value == wanted) {
                return true;
            }
        }
        return false;
    }

    private static HeterogeneousCsr build() throws IOException {
        return HeterogeneousCsr.builder(
                        GraphReader.open(
                                Path.of("..", "..", "testing", "ldbc_sample", "parquet")
                                        .resolve("ldbc_sample.graph.yml")
                                        .toUri(),
                                new LocalFileSystemStringGraphInfoLoader(),
                                new LocalStorage(),
                                new ParquetPhysicalReader(new LocalStorage())))
                .addVertexType("person")
                .addEdgeType("person", "knows", "person")
                .direction(CsrDirection.UNDIRECTED)
                .build();
    }
}
