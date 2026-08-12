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

package org.apache.graphar.integration.ignite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.reader.GraphReader;
import org.apache.graphar.reader.OrderedSourceEdgeReader;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.SeekableInput;
import org.apache.graphar.storage.Storage;
import org.apache.graphar.storage.local.LocalStorage;
import org.apache.graphar.writer.GraphWriter;
import org.apache.graphar.writer.TopologyEdge;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Test;

/** Real two-node embedded Ignite proof plus a deterministic warmed three-hop benchmark. */
public class IgniteCsrStoreIntegrationTest {
    private static final String REGION = "graphar-offheap";
    private static final String CACHE = "graphar-csr-test";

    @Test
    public void loadsGrapharIntoOffHeapCsrAndRunsColocatedThreeHopTraversal() throws Exception {
        Path workRoot = Files.createTempDirectory("graphar-ignite-");
        String suffix = UUID.randomUUID().toString();
        String nodeOneName = "graphar-ignite-one-" + suffix;
        String nodeTwoName = "graphar-ignite-two-" + suffix;
        TcpDiscoveryVmIpFinder finder =
                new TcpDiscoveryVmIpFinder(true).setAddresses(List.of("127.0.0.1:48500..48520"));
        Ignite nodeOne = null;
        Ignite nodeTwo = null;
        Path generatedRoot = null;
        try {
            nodeOne =
                    Ignition.start(
                            configuration(nodeOneName, workRoot.resolve("one"), finder, 48500));
            nodeTwo =
                    Ignition.start(
                            configuration(nodeTwoName, workRoot.resolve("two"), finder, 48501));
            assertEquals(2, nodeOne.cluster().nodes().size());

            OrderedSourceEdgeReader sourceEdges = openFixture();
            GraphInfo graphInfo = fixtureGraphInfo();
            EdgeInfo edgeInfo = graphInfo.getEdgeInfo("person", "knows", "person");
            generatedRoot = Files.createTempDirectory("graphar-indexed-parquet-");
            IndexedFixture indexed =
                    writeIndexedFixture(graphInfo, edgeInfo, generatedRoot, sourceEdges);
            OrderedSourceEdgeReader edges = indexed.reader;
            IgniteCsrStore first = new IgniteCsrStore(nodeOne, CACHE, REGION, 128);
            IgniteCsrStore second = new IgniteCsrStore(nodeTwo, CACHE, REGION, 128);
            IgniteCsrStore.LoadResult load = first.load("fixture-snapshot-a", edges);
            assertEquals(903L, load.vertexCount());
            assertEquals(6626L, load.edgeCount());
            assertEquals(8, load.shardCount());

            int remoteShard = shardOwnedBy(nodeOne, first, load.snapshotId(), load.shardCount());
            assertFalse(first.isLocalOffHeap(load.snapshotId(), remoteShard));
            assertTrue(second.isLocalOffHeap(load.snapshotId(), remoteShard));
            assertEquals(
                    first.primaryNodeId(load.snapshotId(), remoteShard),
                    first.neighborLookupNodeId(
                            load.snapshotId(), (long) remoteShard * first.verticesPerShard()));
            assertEquals(expectedNeighbors297(), asList(first.neighbors(load.snapshotId(), 297L)));

            long started = System.nanoTime();
            IgniteCsrStore.TraversalResult result = null;
            int iterations = 12;
            for (int iteration = 0; iteration < iterations; iteration++) {
                result = first.traverse(load.snapshotId(), List.of(297L), 3, 903);
            }
            long elapsedNanos = System.nanoTime() - started;
            assertEquals(List.of(1, 53, 219, 311), result.frontierSizes());
            assertEquals(311, result.frontier().size());
            assertTrue(result.shardCallsPerHop().get(2) <= load.shardCount());
            System.out.println(
                    "Ignite CSR 3-hop benchmark: iterations="
                            + iterations
                            + ", total_ms="
                            + elapsedNanos / 1_000_000.0
                            + ", avg_ms="
                            + elapsedNanos / 1_000_000.0 / iterations
                            + ", edges="
                            + load.edgeCount()
                            + ", shards="
                            + load.shardCount()
                            + ", affinity_jobs_per_hop="
                            + result.shardCallsPerHop());

            long parquetStarted = System.nanoTime();
            List<Integer> parquetSizes = null;
            indexed.storage.reset();
            indexed.physicalReader.reset();
            for (int iteration = 0; iteration < iterations; iteration++) {
                parquetSizes = traverseGraphAr(edges, List.of(297L), 3, 903);
            }
            long parquetElapsedNanos = System.nanoTime() - parquetStarted;
            assertEquals(List.of(1, 53, 219, 311), parquetSizes);
            assertTrue(indexed.storage.opens > 0);
            assertTrue("Expected adjacency-chunk batching", indexed.storage.opens < 300);
            assertTrue(indexed.storage.bytes > 0);
            System.out.println(
                    "GraphAr indexed Parquet FS 3-hop benchmark: iterations="
                            + iterations
                            + ", total_ms="
                            + parquetElapsedNanos / 1_000_000.0
                            + ", avg_ms="
                            + parquetElapsedNanos / 1_000_000.0 / iterations
                            + ", neighbor_lookups_per_hop=[1, 53, 219]");
            System.out.println(
                    "GraphAr indexed Parquet FS physical IO: opens="
                            + indexed.storage.opens
                            + ", seeks="
                            + indexed.storage.seeks
                            + ", reads="
                            + indexed.storage.reads
                            + ", bytes="
                            + indexed.storage.bytes
                            + ", storage_io_ms="
                            + indexed.storage.ioNanos / 1_000_000.0);
            System.out.println(
                    "GraphAr indexed Parquet Java work: reader_open_ms="
                            + indexed.physicalReader.openNanos / 1_000_000.0
                            + ", cursor_next_ms="
                            + indexed.physicalReader.nextNanos / 1_000_000.0);

        } finally {
            if (nodeTwo != null) {
                Ignition.stop(nodeTwoName, true);
            }
            if (nodeOne != null) {
                Ignition.stop(nodeOneName, true);
            }
            deleteTree(workRoot);
            if (generatedRoot != null) {
                deleteTree(generatedRoot);
            }
        }
    }

    private static int shardOwnedBy(
            Ignite requester, IgniteCsrStore store, String snapshotId, int shardCount) {
        for (int shard = 0; shard < shardCount; shard++) {
            if (!store.primaryNodeId(snapshotId, shard)
                    .equals(requester.cluster().localNode().id())) {
                return shard;
            }
        }
        throw new AssertionError(
                "Expected a two-node partitioned cache to assign a remote primary shard.");
    }

    private static IgniteConfiguration configuration(
            String name, Path workDirectory, TcpDiscoveryVmIpFinder finder, int discoveryPort) {
        DataRegionConfiguration region =
                new DataRegionConfiguration()
                        .setName(REGION)
                        .setInitialSize(32L * 1024 * 1024)
                        .setMaxSize(64L * 1024 * 1024)
                        .setPersistenceEnabled(false)
                        .setMetricsEnabled(true);
        DataStorageConfiguration storage =
                new DataStorageConfiguration()
                        .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setName("graphar-default")
                                        .setInitialSize(32L * 1024 * 1024)
                                        .setMaxSize(64L * 1024 * 1024)
                                        .setPersistenceEnabled(false))
                        .setDataRegionConfigurations(region);
        return new IgniteConfiguration()
                .setIgniteInstanceName(name)
                .setWorkDirectory(workDirectory.toString())
                .setLocalHost("127.0.0.1")
                .setDataStorageConfiguration(storage)
                .setPeerClassLoadingEnabled(false)
                .setDiscoverySpi(
                        new TcpDiscoverySpi()
                                .setIpFinder(finder)
                                .setLocalPort(discoveryPort)
                                .setLocalPortRange(1));
    }

    private static OrderedSourceEdgeReader openFixture() throws IOException {
        Path root = Path.of("..", "..", "testing", "ldbc_sample", "parquet");
        GraphReader graph =
                GraphReader.open(
                        root.resolve("ldbc_sample.graph.yml").toUri(),
                        new LocalFileSystemStringGraphInfoLoader(),
                        new LocalStorage(),
                        new ParquetPhysicalReader(new LocalStorage()));
        return graph.edge("person", "knows", "person");
    }

    private static GraphInfo fixtureGraphInfo() throws IOException {
        Path root = Path.of("..", "..", "testing", "ldbc_sample", "parquet");
        return new LocalFileSystemStringGraphInfoLoader()
                .loadGraphInfo(root.resolve("ldbc_sample.graph.yml").toUri());
    }

    private static IndexedFixture writeIndexedFixture(
            GraphInfo graphInfo, EdgeInfo edgeInfo, Path root, OrderedSourceEdgeReader source)
            throws IOException {
        List<TopologyEdge> topology = new ArrayList<>();
        try (org.apache.graphar.reader.EdgeCursor cursor = source.scanEdges()) {
            while (cursor.next()) {
                topology.add(new TopologyEdge(cursor.source(), cursor.destination()));
            }
        }
        LocalStorage storage = new LocalStorage();
        new GraphWriter(
                        graphInfo,
                        root.toUri(),
                        storage,
                        new org.apache.graphar.io.parquet.ParquetPhysicalWriter(storage))
                .writeOrderedSourceTopology(edgeInfo, source.vertexCount(), topology);
        CountingStorage counting = new CountingStorage(storage);
        TimingPhysicalReader physicalReader =
                new TimingPhysicalReader(new ParquetPhysicalReader(counting));
        return new IndexedFixture(
                new GraphReader(graphInfo, root.toUri(), counting, physicalReader)
                        .edge("person", "knows", "person"),
                counting,
                physicalReader);
    }

    private static List<Integer> traverseGraphAr(
            OrderedSourceEdgeReader reader, List<Long> seeds, int hops, int maximum)
            throws IOException {
        java.util.Set<Long> frontier = new java.util.LinkedHashSet<>(seeds);
        List<Integer> sizes = new ArrayList<>();
        sizes.add(frontier.size());
        for (int hop = 0; hop < hops; hop++) {
            java.util.Set<Long> next = new java.util.LinkedHashSet<>();
            for (List<Long> neighbors : reader.neighbors(frontier).values()) {
                for (long destination : neighbors) {
                    next.add(destination);
                    if (next.size() > maximum) {
                        throw new IllegalArgumentException("Traversal frontier exceeds limit.");
                    }
                }
            }
            frontier = next;
            sizes.add(frontier.size());
        }
        return sizes;
    }

    private static final class IndexedFixture {
        private final OrderedSourceEdgeReader reader;
        private final CountingStorage storage;
        private final TimingPhysicalReader physicalReader;

        private IndexedFixture(
                OrderedSourceEdgeReader reader,
                CountingStorage storage,
                TimingPhysicalReader physicalReader) {
            this.reader = reader;
            this.storage = storage;
            this.physicalReader = physicalReader;
        }
    }

    private static final class TimingPhysicalReader implements PhysicalReader {
        private final PhysicalReader delegate;
        private long openNanos;
        private long nextNanos;

        private TimingPhysicalReader(PhysicalReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public java.util.Set<org.apache.graphar.io.ReadCapability> capabilities() {
            return delegate.capabilities();
        }

        @Override
        public ReadResult read(ReadRequest request) throws IOException {
            long started = System.nanoTime();
            ReadResult result;
            try {
                result = delegate.read(request);
            } finally {
                openNanos += System.nanoTime() - started;
            }
            return new ReadResult(
                    request, new TimingBatchCursor(result.cursor(), this), result.report());
        }

        private void reset() {
            openNanos = 0;
            nextNanos = 0;
        }
    }

    private static final class TimingBatchCursor implements BatchCursor {
        private final BatchCursor delegate;
        private final TimingPhysicalReader timing;

        private TimingBatchCursor(BatchCursor delegate, TimingPhysicalReader timing) {
            this.delegate = delegate;
            this.timing = timing;
        }

        @Override
        public boolean next() throws IOException {
            long started = System.nanoTime();
            try {
                return delegate.next();
            } finally {
                timing.nextNanos += System.nanoTime() - started;
            }
        }

        @Override
        public RecordBatch batch() {
            return delegate.batch();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class CountingStorage implements Storage {
        private final Storage delegate;
        private long opens;
        private long seeks;
        private long reads;
        private long bytes;
        private long ioNanos;

        private CountingStorage(Storage delegate) {
            this.delegate = delegate;
        }

        @Override
        public InputFile inputFile(URI uri) {
            return new InputFile() {
                @Override
                public URI uri() {
                    return delegate.inputFile(uri).uri();
                }

                @Override
                public long size() throws IOException {
                    long started = System.nanoTime();
                    try {
                        return delegate.inputFile(uri).size();
                    } finally {
                        ioNanos += System.nanoTime() - started;
                    }
                }

                @Override
                public SeekableInput open() throws IOException {
                    opens++;
                    long started = System.nanoTime();
                    SeekableInput input;
                    try {
                        input = delegate.inputFile(uri).open();
                    } finally {
                        ioNanos += System.nanoTime() - started;
                    }
                    return new SeekableInput() {
                        @Override
                        public long position() throws IOException {
                            return input.position();
                        }

                        @Override
                        public void seek(long newPosition) throws IOException {
                            seeks++;
                            long started = System.nanoTime();
                            try {
                                input.seek(newPosition);
                            } finally {
                                ioNanos += System.nanoTime() - started;
                            }
                        }

                        @Override
                        public int read(ByteBuffer destination) throws IOException {
                            long started = System.nanoTime();
                            int count;
                            try {
                                count = input.read(destination);
                            } finally {
                                ioNanos += System.nanoTime() - started;
                            }
                            reads++;
                            if (count > 0) {
                                bytes += count;
                            }
                            return count;
                        }

                        @Override
                        public void close() throws IOException {
                            input.close();
                        }
                    };
                }
            };
        }

        @Override
        public OutputFile outputFile(URI uri) {
            return delegate.outputFile(uri);
        }

        @Override
        public boolean exists(URI uri) throws IOException {
            return delegate.exists(uri);
        }

        private void reset() {
            opens = 0;
            seeks = 0;
            reads = 0;
            bytes = 0;
            ioNanos = 0;
        }
    }

    private static List<Long> asList(long[] values) {
        List<Long> result = new ArrayList<>();
        for (long value : values) {
            result.add(value);
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

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            for (Path path :
                    paths.sorted(Comparator.reverseOrder())
                            .collect(java.util.stream.Collectors.toList())) {
                Files.delete(path);
            }
        }
    }
}
