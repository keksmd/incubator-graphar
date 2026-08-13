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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

/**
 * Pins the refresh contract a serving API depends on: a rebuild publishes in one step, a request
 * that started before it keeps answering from the graph it started with, and a rebuild that fails
 * leaves the previous graph serving.
 */
public class GraphProjectionRefreshTest {
    private static final Duration ONE_HOUR = Duration.ofHours(1);

    @Test
    public void servesTheGraphItBuiltAtLoad() throws Exception {
        GraphProjection projection = GraphProjection.load(() -> build(CsrDirection.UNDIRECTED));

        GraphProjection.Snapshot snapshot = projection.current();
        assertEquals(1L, snapshot.generation());
        assertEquals(List.of("person"), snapshot.projection().vertexTypes());
        assertTrue(snapshot.projection().vertexCount() > 0);
        assertSame(snapshot, projection.current());
    }

    @Test
    public void aRefreshPublishesADifferentGraphInOneStep() throws Exception {
        AtomicReference<CsrDirection> direction = new AtomicReference<>(CsrDirection.OUTGOING);
        GraphProjection projection = GraphProjection.load(() -> build(direction.get()));

        GraphProjection.Snapshot before = projection.current();
        direction.set(CsrDirection.UNDIRECTED);
        GraphProjection.Snapshot after = projection.refresh();

        assertEquals(2L, after.generation());
        assertSame(after, projection.current());
        assertNotEquals(before.projection().edgeCount(), after.projection().edgeCount());
        assertEquals(
                "the snapshot handed out earlier must keep its own graph",
                before.projection().edgeCount() * 2,
                after.projection().edgeCount());
    }

    @Test
    public void aRequestHoldingASnapshotIsUnaffectedByARefreshThatLandsUnderIt() throws Exception {
        AtomicReference<CsrDirection> direction = new AtomicReference<>(CsrDirection.OUTGOING);
        GraphProjection projection = GraphProjection.load(() -> build(direction.get()));

        GraphProjection.Snapshot serving = projection.current();
        long vertex = busiestVertex(serving.projection().csr());
        long[] neighborsBefore = serving.projection().neighbors(vertex);

        direction.set(CsrDirection.UNDIRECTED);
        projection.refresh();

        assertArrayEqualsLong(neighborsBefore, serving.projection().neighbors(vertex));
        assertTrue(
                "the refreshed graph must actually differ at that vertex",
                projection.current().projection().neighbors(vertex).length
                        > neighborsBefore.length);
    }

    @Test
    public void concurrentReadersOnlyEverSeeACompleteGraph() throws Exception {
        AtomicReference<CsrDirection> direction = new AtomicReference<>(CsrDirection.OUTGOING);
        GraphProjection projection = GraphProjection.load(() -> build(direction.get()));
        long outgoingEdges = projection.current().projection().edgeCount();

        int readerCount = 4;
        CountDownLatch started = new CountDownLatch(readerCount);
        AtomicInteger failures = new AtomicInteger();
        AtomicReference<Boolean> stop = new AtomicReference<>(Boolean.FALSE);
        List<Thread> readers = new ArrayList<>();
        for (int reader = 0; reader < readerCount; reader++) {
            Thread thread =
                    new Thread(
                            () -> {
                                started.countDown();
                                while (!stop.get()) {
                                    GraphProjection.Snapshot snapshot = projection.current();
                                    HeterogeneousCsr graph = snapshot.projection();
                                    long edges = graph.edgeCount();
                                    if (edges != outgoingEdges && edges != outgoingEdges * 2) {
                                        failures.incrementAndGet();
                                    }
                                    TraversalResult result =
                                            BoundedTraversal.neighborhood(graph.csr(), 0, 2, 50);
                                    if (result.size() < 1) {
                                        failures.incrementAndGet();
                                    }
                                }
                            });
            readers.add(thread);
            thread.start();
        }
        started.await();

        for (int round = 0; round < 6; round++) {
            direction.set(round % 2 == 0 ? CsrDirection.UNDIRECTED : CsrDirection.OUTGOING);
            projection.refresh();
        }
        stop.set(Boolean.TRUE);
        for (Thread thread : readers) {
            thread.join();
        }

        assertEquals(0, failures.get());
        assertEquals(7L, projection.current().generation());
    }

    @Test
    public void aFailedRebuildKeepsThePreviousGraphServing() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        GraphProjection projection =
                GraphProjection.load(
                        () -> {
                            if (attempts.getAndIncrement() > 0) {
                                throw new IOException("dataset is unavailable");
                            }
                            return build(CsrDirection.UNDIRECTED);
                        });
        GraphProjection.Snapshot before = projection.current();

        try {
            projection.refresh();
            fail("a failed rebuild must not be silent");
        } catch (IOException expected) {
            assertEquals("dataset is unavailable", expected.getMessage());
        }

        assertSame(before, projection.current());
        assertEquals(1L, projection.current().generation());
    }

    @Test
    public void staleIsMeasuredAgainstTheStartOfTheBuild() throws Exception {
        MovingClock clock = new MovingClock(Instant.parse("2026-01-01T00:00:00Z"));
        AtomicInteger builds = new AtomicInteger();
        GraphProjection projection =
                GraphProjection.load(
                        () -> {
                            builds.incrementAndGet();
                            return build(CsrDirection.UNDIRECTED);
                        },
                        clock);

        assertEquals(Duration.ZERO, projection.current().age(clock));
        clock.advance(Duration.ofMinutes(59));
        assertTrue(!projection.isStale(ONE_HOUR));
        assertEquals(1, builds.get());
        projection.refreshIfStale(ONE_HOUR);
        assertEquals("a fresh index must not be rebuilt", 1, builds.get());

        clock.advance(Duration.ofMinutes(2));
        assertTrue(projection.isStale(ONE_HOUR));
        GraphProjection.Snapshot refreshed = projection.refreshIfStale(ONE_HOUR);
        assertEquals(2, builds.get());
        assertEquals(2L, refreshed.generation());
        assertEquals(Duration.ZERO, refreshed.age(clock));
    }

    private static long busiestVertex(CsrGraph csr) {
        long best = 0;
        long bestDegree = -1;
        for (long vertex = 0; vertex < csr.vertexCount(); vertex++) {
            long degree = csr.degree(vertex);
            if (degree > bestDegree) {
                bestDegree = degree;
                best = vertex;
            }
        }
        return best;
    }

    private static void assertArrayEqualsLong(long[] expected, long[] actual) {
        assertEquals(expected.length, actual.length);
        for (int index = 0; index < expected.length; index++) {
            assertEquals(expected[index], actual[index]);
        }
    }

    private static HeterogeneousCsr build(CsrDirection direction) throws IOException {
        GraphReader graph =
                GraphReader.open(
                        fixturePath().resolve("ldbc_sample.graph.yml").toUri(),
                        new LocalFileSystemStringGraphInfoLoader(),
                        new LocalStorage(),
                        new ParquetPhysicalReader(new LocalStorage()));
        return HeterogeneousCsr.builder(graph)
                .addVertexType("person")
                .addEdgeType("person", "knows", "person")
                .direction(direction)
                .build();
    }

    private static Path fixturePath() {
        return Path.of("..", "..", "testing", "ldbc_sample", "parquet");
    }

    /** A clock the test moves by hand, so index age is asserted without waiting. */
    private static final class MovingClock extends Clock {
        private Instant now;

        private MovingClock(Instant now) {
            this.now = now;
        }

        private void advance(Duration step) {
            now = now.plus(step);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
