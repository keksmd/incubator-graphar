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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.reader.GraphReader;
import org.apache.graphar.reader.OrderedSourceEdgeReader;
import org.apache.graphar.storage.local.LocalStorage;
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
        try {
            nodeOne =
                    Ignition.start(
                            configuration(nodeOneName, workRoot.resolve("one"), finder, 48500));
            nodeTwo =
                    Ignition.start(
                            configuration(nodeTwoName, workRoot.resolve("two"), finder, 48501));
            assertEquals(2, nodeOne.cluster().nodes().size());

            OrderedSourceEdgeReader edges = openFixture();
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

        } finally {
            if (nodeTwo != null) {
                Ignition.stop(nodeTwoName, true);
            }
            if (nodeOne != null) {
                Ignition.stop(nodeOneName, true);
            }
            deleteTree(workRoot);
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
