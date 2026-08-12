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

package org.apache.graphar.integration.iceberg.ignite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.graphar.integration.iceberg.IcebergFileIOStorage;
import org.apache.graphar.integration.ignite.IgniteCsrStore;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.junit.Test;

/** Proves a globally ordered pinned Iceberg snapshot streams directly into two-node Ignite CSR. */
public class IcebergIgniteCsrLoaderIntegrationTest {
    private static final String REGION = "graphar-iceberg-offheap";
    private static final String CACHE = "graphar-iceberg-csr";
    private static final int VERTICES = 512;
    private static final Schema TOPOLOGY_SCHEMA =
            new Schema(
                    List.of(
                            new Field("source", ColumnType.of(ColumnType.Kind.INT64), false),
                            new Field("destination", ColumnType.of(ColumnType.Kind.INT64), false)));

    @Test
    public void streamsPinnedGloballyOrderedSnapshotIntoOffHeapCsr() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize(
                "iceberg-ignite",
                Map.of(CatalogProperties.WAREHOUSE_LOCATION, "s3://iceberg-ignite/warehouse"));
        catalog.createNamespace(Namespace.of("graph"));
        Table table =
                catalog.createTable(
                        TableIdentifier.of("graph", "ordered_edges"),
                        new org.apache.iceberg.Schema(
                                Types.NestedField.required(1, "source", Types.LongType.get()),
                                Types.NestedField.required(2, "destination", Types.LongType.get())),
                        PartitionSpec.unpartitioned());
        append(table, URI.create(table.location() + "/data/ordered.parquet"), orderedRows());
        long snapshotA = table.currentSnapshot().snapshotId();
        append(
                table,
                URI.create(table.location() + "/data/newer.parquet"),
                List.of(new TopologyRow(1, 999_999)));
        assertNotEquals(snapshotA, table.currentSnapshot().snapshotId());

        Path workRoot = Files.createTempDirectory("graphar-iceberg-ignite-");
        String suffix = UUID.randomUUID().toString();
        String firstName = "graphar-iceberg-ignite-one-" + suffix;
        String secondName = "graphar-iceberg-ignite-two-" + suffix;
        TcpDiscoveryVmIpFinder finder =
                new TcpDiscoveryVmIpFinder(true).setAddresses(List.of("127.0.0.1:48600..48620"));
        Ignite first = null;
        Ignite second = null;
        try {
            first =
                    Ignition.start(
                            configuration(firstName, workRoot.resolve("one"), finder, 48600));
            second =
                    Ignition.start(
                            configuration(secondName, workRoot.resolve("two"), finder, 48601));
            assertEquals(2, first.cluster().nodes().size());
            IgniteCsrStore store = new IgniteCsrStore(first, CACHE, REGION, 64, 1);

            long started = System.nanoTime();
            IcebergIgniteCsrLoader.LoadResult load =
                    new IcebergIgniteCsrLoader()
                            .load(table, snapshotA, VERTICES, "source", "destination", store);
            long loadNanos = System.nanoTime() - started;
            assertEquals(snapshotA, load.icebergSnapshotId());
            assertEquals(VERTICES, load.csr().vertexCount());
            assertEquals((long) VERTICES * 4, load.csr().edgeCount());
            assertEquals(
                    List.of(2L, 3L, 4L, 5L), asList(store.neighbors("iceberg-" + snapshotA, 1)));

            long traversalStarted = System.nanoTime();
            IgniteCsrStore.TraversalResult traversal = null;
            int iterations = 20;
            for (int iteration = 0; iteration < iterations; iteration++) {
                traversal = store.traverse("iceberg-" + snapshotA, List.of(1L), 3, 512);
            }
            long traversalNanos = System.nanoTime() - traversalStarted;
            assertEquals(List.of(1, 4, 7, 10), traversal.frontierSizes());
            System.out.println(
                    "Iceberg ordered snapshot -> Ignite CSR: rows="
                            + load.csr().edgeCount()
                            + ", shards="
                            + load.csr().shardCount()
                            + ", load_ms="
                            + loadNanos / 1_000_000.0
                            + ", 3-hop_avg_ms="
                            + traversalNanos / 1_000_000.0 / iterations
                            + ", affinity_jobs_per_hop="
                            + traversal.shardCallsPerHop());
        } finally {
            if (second != null) {
                Ignition.stop(secondName, true);
            }
            if (first != null) {
                Ignition.stop(firstName, true);
            }
            deleteTree(workRoot);
            catalog.close();
        }
    }

    @Test
    public void rejectsSnapshotWhoseRowsAreNotGloballySourceOrdered() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize(
                "iceberg-ignite-unsorted",
                Map.of(CatalogProperties.WAREHOUSE_LOCATION, "s3://iceberg-ignite/unsorted"));
        catalog.createNamespace(Namespace.of("graph"));
        Table table =
                catalog.createTable(
                        TableIdentifier.of("graph", "unsorted_edges"),
                        new org.apache.iceberg.Schema(
                                Types.NestedField.required(1, "source", Types.LongType.get()),
                                Types.NestedField.required(2, "destination", Types.LongType.get())),
                        PartitionSpec.unpartitioned());
        append(
                table,
                URI.create(table.location() + "/data/unsorted.parquet"),
                List.of(new TopologyRow(4, 40), new TopologyRow(2, 20)));
        Path workRoot = Files.createTempDirectory("graphar-iceberg-unsorted-");
        String nodeName = "graphar-iceberg-unsorted-" + UUID.randomUUID();
        Ignite node = null;
        try {
            node =
                    Ignition.start(
                            configuration(
                                    nodeName,
                                    workRoot,
                                    new TcpDiscoveryVmIpFinder(true)
                                            .setAddresses(List.of("127.0.0.1:48700..48710")),
                                    48700));
            try {
                new IcebergIgniteCsrLoader()
                        .load(
                                table,
                                table.currentSnapshot().snapshotId(),
                                8,
                                "source",
                                "destination",
                                new IgniteCsrStore(node, CACHE + "-unsorted", REGION, 8));
                fail("Expected globally unordered snapshot rejection.");
            } catch (IllegalArgumentException expected) {
                assertEquals(
                        "GraphAr source scan is not sorted and in bounds.", expected.getMessage());
            }
        } finally {
            if (node != null) {
                Ignition.stop(nodeName, true);
            }
            deleteTree(workRoot);
            catalog.close();
        }
    }

    private static void append(Table table, URI uri, List<TopologyRow> rows) throws IOException {
        IcebergFileIOStorage storage = new IcebergFileIOStorage(table.io());
        new ParquetPhysicalWriter(storage)
                .write(
                        new WriteRequest(uri, TOPOLOGY_SCHEMA, WriteMode.CREATE_NEW),
                        new SingleBatchCursor(new Rows(rows)));
        table.newAppend()
                .appendFile(
                        DataFiles.builder(table.spec())
                                .withPath(uri.toString())
                                .withFormat(FileFormat.PARQUET)
                                .withFileSizeInBytes(storage.inputFile(uri).size())
                                .withRecordCount(rows.size())
                                .build())
                .commit();
    }

    private static List<TopologyRow> orderedRows() {
        List<TopologyRow> rows = new ArrayList<>();
        for (long source = 0; source < VERTICES; source++) {
            for (long destination = source + 1;
                    destination <= Math.min(source + 4, VERTICES - 1);
                    destination++) {
                rows.add(new TopologyRow(source, destination));
            }
            while (rows.size() < (source + 1) * 4) {
                rows.add(new TopologyRow(source, source));
            }
        }
        return rows;
    }

    private static List<Long> asList(long[] values) {
        List<Long> result = new ArrayList<>();
        for (long value : values) {
            result.add(value);
        }
        return result;
    }

    private static IgniteConfiguration configuration(
            String name, Path workDirectory, TcpDiscoveryVmIpFinder finder, int port) {
        DataRegionConfiguration region =
                new DataRegionConfiguration()
                        .setName(REGION)
                        .setInitialSize(32L * 1024 * 1024)
                        .setMaxSize(64L * 1024 * 1024)
                        .setPersistenceEnabled(false);
        return new IgniteConfiguration()
                .setIgniteInstanceName(name)
                .setWorkDirectory(workDirectory.toString())
                .setLocalHost("127.0.0.1")
                .setDataStorageConfiguration(
                        new DataStorageConfiguration()
                                .setDefaultDataRegionConfiguration(
                                        new DataRegionConfiguration()
                                                .setName("graphar-default")
                                                .setInitialSize(32L * 1024 * 1024)
                                                .setMaxSize(64L * 1024 * 1024)
                                                .setPersistenceEnabled(false))
                                .setDataRegionConfigurations(region))
                .setPeerClassLoadingEnabled(false)
                .setDiscoverySpi(
                        new TcpDiscoverySpi()
                                .setIpFinder(finder)
                                .setLocalPort(port)
                                .setLocalPortRange(1));
    }

    private static void deleteTree(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
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

    private static final class TopologyRow implements org.apache.graphar.io.Row {
        private final long source;
        private final long destination;

        private TopologyRow(long source, long destination) {
            this.source = source;
            this.destination = destination;
        }

        @Override
        public Object value(int index) {
            return index == 0 ? source : destination;
        }
    }

    private static final class Rows implements RecordBatch {
        private final List<TopologyRow> rows;

        private Rows(List<TopologyRow> rows) {
            this.rows = rows;
        }

        @Override
        public Schema schema() {
            return TOPOLOGY_SCHEMA;
        }

        @Override
        public int rowCount() {
            return rows.size();
        }

        @Override
        public org.apache.graphar.io.Row row(int index) {
            return rows.get(index);
        }
    }

    private static final class SingleBatchCursor implements BatchCursor {
        private final RecordBatch batch;
        private boolean read;

        private SingleBatchCursor(RecordBatch batch) {
            this.batch = batch;
        }

        @Override
        public boolean next() {
            if (read) {
                return false;
            }
            read = true;
            return true;
        }

        @Override
        public RecordBatch batch() {
            if (!read) {
                throw new IllegalStateException("No current batch. Call next() first.");
            }
            return batch;
        }

        @Override
        public void close() {}
    }
}
