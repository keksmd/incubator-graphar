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
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadCapability;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.Storage;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

/**
 * Pins the boundary between GraphAr reading and pair reachability serving.
 *
 * <p>A batch head-to-head request asks whether two identifiers are connected within a hop bound. It
 * is answered entirely from the materialized adjacency: the dataset is read once when the CSR is
 * built, and every pair in the batch is then served without touching storage again. These tests
 * assert that by counting storage and physical-reader calls around the batch, so the claim rests on
 * observed IO rather than on inspection of the call graph.
 */
public class BatchHeadToHeadFixtureTest {
    private static final long MAX_VERTICES = 10_000L;
    private static final long MAX_EDGES = 100_000L;
    private static final int BATCH_SIZE = 200;
    private static final int MAX_HOPS = 3;
    private static final long PAIR_SEED = 42L;

    @Test
    public void servesATwoHundredPairBatchWithoutReadingTheDatasetAgain() throws Exception {
        CountingStorage storage = new CountingStorage(new LocalStorage());
        CountingPhysicalReader physical =
                new CountingPhysicalReader(new ParquetPhysicalReader(storage));
        CsrGraph csr =
                GraphReader.open(
                                fixturePath().resolve("ldbc_sample.graph.yml").toUri(),
                                new LocalFileSystemStringGraphInfoLoader(),
                                storage,
                                physical)
                        .edge("person", "knows", "person")
                        .materializeCsr(MAX_VERTICES, MAX_EDGES, CsrDirection.UNDIRECTED);

        long filesAfterBuild = storage.inputFiles();
        long readsAfterBuild = physical.reads();
        assertTrue("building the CSR must read the dataset", readsAfterBuild > 0);

        List<long[]> batch = batch(csr.vertexCount());
        int connected = 0;
        for (long[] pair : batch) {
            if (hops(csr, pair[0], pair[1], MAX_HOPS) >= 0) {
                connected++;
            }
        }

        assertEquals(BATCH_SIZE, batch.size());
        assertTrue("the batch must contain reachable pairs", connected > 0);
        assertTrue("the batch must contain unreachable pairs", connected < BATCH_SIZE);
        assertEquals(filesAfterBuild, storage.inputFiles());
        assertEquals(readsAfterBuild, physical.reads());
    }

    @Test
    public void unboundedReachabilityAgreesWithAnIndependentUnionFind() throws Exception {
        CsrGraph csr = csr();
        int[] parent = unionFind(csr);
        Map<Long, long[]> componentByStart = new HashMap<>();

        for (long[] pair : batch(csr.vertexCount())) {
            long[] reached =
                    componentByStart.computeIfAbsent(
                            pair[0],
                            start -> {
                                TraversalResult component =
                                        BoundedTraversal.component(csr, start, Integer.MAX_VALUE);
                                assertFalse(component.truncated());
                                long[] sorted = component.vertices().clone();
                                Arrays.sort(sorted);
                                return sorted;
                            });
            boolean sameRoot =
                    root(parent, Math.toIntExact(pair[0]))
                            == root(parent, Math.toIntExact(pair[1]));
            assertEquals(
                    "pair " + pair[0] + "/" + pair[1],
                    sameRoot,
                    Arrays.binarySearch(reached, pair[1]) >= 0);
        }
    }

    @Test
    public void theHopBoundHidesPairsThatAreConnectedOnlyFurtherAway() throws Exception {
        CsrGraph csr = csr();
        int[] parent = unionFind(csr);
        int hiddenByBound = 0;

        for (long[] pair : batch(csr.vertexCount())) {
            int hops = hops(csr, pair[0], pair[1], MAX_HOPS);
            boolean sameRoot =
                    root(parent, Math.toIntExact(pair[0]))
                            == root(parent, Math.toIntExact(pair[1]));
            if (hops >= 0) {
                assertTrue(hops <= MAX_HOPS);
                assertTrue("reachable within the bound implies one component", sameRoot);
            } else if (sameRoot) {
                hiddenByBound++;
            }
        }

        assertTrue(
                "the hop bound must be observable in the answer, not only in the contract",
                hiddenByBound > 0);
    }

    @Test
    public void aPairIsSymmetricUnderTheUndirectedAdjacency() throws Exception {
        CsrGraph csr = csr();

        for (long[] pair : batch(csr.vertexCount())) {
            assertEquals(
                    "pair " + pair[0] + "/" + pair[1],
                    hops(csr, pair[0], pair[1], MAX_HOPS),
                    hops(csr, pair[1], pair[0], MAX_HOPS));
        }
    }

    private static int hops(CsrGraph csr, long from, long to, int maxHops) {
        TraversalResult result =
                BoundedTraversal.neighborhood(csr, from, maxHops, Integer.MAX_VALUE);
        long[] vertices = result.vertices();
        for (int position = 0; position < vertices.length; position++) {
            if (vertices[position] == to) {
                return result.depths()[position];
            }
        }
        return -1;
    }

    private static List<long[]> batch(long vertexCount) {
        Random random = new Random(PAIR_SEED);
        int bound = Math.toIntExact(vertexCount);
        List<long[]> pairs = new ArrayList<>(BATCH_SIZE);
        while (pairs.size() < BATCH_SIZE) {
            long left = random.nextInt(bound);
            long right = random.nextInt(bound);
            if (left != right) {
                pairs.add(new long[] {left, right});
            }
        }
        return pairs;
    }

    private static int[] unionFind(CsrGraph csr) {
        int vertexCount = Math.toIntExact(csr.vertexCount());
        int[] parent = new int[vertexCount];
        for (int vertex = 0; vertex < vertexCount; vertex++) {
            parent[vertex] = vertex;
        }
        long[] offsets = csr.offsets();
        long[] destinations = csr.destinations();
        for (int vertex = 0; vertex < vertexCount; vertex++) {
            int from = Math.toIntExact(offsets[vertex]);
            int to = Math.toIntExact(offsets[vertex + 1]);
            for (int entry = from; entry < to; entry++) {
                int left = root(parent, vertex);
                int right = root(parent, Math.toIntExact(destinations[entry]));
                if (left != right) {
                    parent[left] = right;
                }
            }
        }
        return parent;
    }

    private static int root(int[] parent, int vertex) {
        while (parent[vertex] != vertex) {
            parent[vertex] = parent[parent[vertex]];
            vertex = parent[vertex];
        }
        return vertex;
    }

    private static CsrGraph csr() throws IOException {
        return GraphReader.open(
                        fixturePath().resolve("ldbc_sample.graph.yml").toUri(),
                        new LocalFileSystemStringGraphInfoLoader(),
                        new LocalStorage(),
                        new ParquetPhysicalReader(new LocalStorage()))
                .edge("person", "knows", "person")
                .materializeCsr(MAX_VERTICES, MAX_EDGES, CsrDirection.UNDIRECTED);
    }

    private static Path fixturePath() {
        return Path.of("..", "..", "testing", "ldbc_sample", "parquet");
    }

    /** Counts every file resolution so a batch can be proven to touch none. */
    private static final class CountingStorage implements Storage {
        private final Storage delegate;
        private long inputFiles;

        private CountingStorage(Storage delegate) {
            this.delegate = delegate;
        }

        long inputFiles() {
            return inputFiles;
        }

        @Override
        public InputFile inputFile(URI uri) {
            inputFiles++;
            return delegate.inputFile(uri);
        }

        @Override
        public OutputFile outputFile(URI uri) {
            return delegate.outputFile(uri);
        }

        @Override
        public boolean exists(URI uri) throws IOException {
            return delegate.exists(uri);
        }
    }

    /** Counts every physical read so a batch can be proven to issue none. */
    private static final class CountingPhysicalReader implements PhysicalReader {
        private final PhysicalReader delegate;
        private long reads;

        private CountingPhysicalReader(PhysicalReader delegate) {
            this.delegate = delegate;
        }

        long reads() {
            return reads;
        }

        @Override
        public Set<ReadCapability> capabilities() {
            return delegate.capabilities();
        }

        @Override
        public ReadResult read(ReadRequest request) throws IOException {
            reads++;
            return delegate.read(request);
        }
    }
}
