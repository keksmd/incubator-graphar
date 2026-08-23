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

package org.apache.graphar.integration.iceberg;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.ValueVector;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.storage.PositionOutput;
import org.apache.graphar.writer.GraphWriter;
import org.apache.graphar.writer.TopologyEdge;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Pins an Iceberg snapshot, builds a bounded in-memory source index, and writes ordered GraphAr
 * topology through an injected GraphAr writer.
 *
 * <p>The table's {@code FileIO} may be Iceberg {@code S3FileIO}; in that case every input and
 * GraphAr output URI is a real {@code s3://} object operation through {@link IcebergFileIOStorage}.
 */
public final class IcebergTopologyExporter {
    /** Immutable provenance for one snapshot-pinned export. */
    public static final class ExportResult {
        private final long snapshotId;
        private final long edgeCount;
        private final URI grapharRoot;

        private ExportResult(long snapshotId, long edgeCount, URI grapharRoot) {
            this.snapshotId = snapshotId;
            this.edgeCount = edgeCount;
            this.grapharRoot = grapharRoot;
        }

        public long snapshotId() {
            return snapshotId;
        }

        public long edgeCount() {
            return edgeCount;
        }

        public URI grapharRoot() {
            return grapharRoot;
        }
    }

    private final long maxInMemoryEdges;

    /** Creates an exporter that rejects a snapshot with more than {@code maxInMemoryEdges}. */
    public IcebergTopologyExporter(long maxInMemoryEdges) {
        if (maxInMemoryEdges < 0) {
            throw new IllegalArgumentException(
                    "Maximum in-memory edge count must be non-negative.");
        }
        this.maxInMemoryEdges = maxInMemoryEdges;
    }

    /**
     * Exports two required INT64 Iceberg columns. The caller supplies the source vertex count so
     * isolated vertices survive the export. Delete files are rejected because scanning raw Parquet
     * files would otherwise silently produce stale adjacency.
     */
    public ExportResult exportOrderedSource(
            Table table,
            long snapshotId,
            EdgeInfo edgeInfo,
            long sourceVertexCount,
            String sourceColumn,
            String destinationColumn,
            URI grapharRoot,
            GraphWriter graphWriter)
            throws IOException {
        Objects.requireNonNull(table, "Iceberg table cannot be null.");
        Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        Objects.requireNonNull(sourceColumn, "Source column cannot be null.");
        Objects.requireNonNull(destinationColumn, "Destination column cannot be null.");
        Objects.requireNonNull(grapharRoot, "GraphAr root cannot be null.");
        Objects.requireNonNull(graphWriter, "Graph writer cannot be null.");
        if (sourceVertexCount < 0 || sourceColumn.isBlank() || destinationColumn.isBlank()) {
            throw new IllegalArgumentException(
                    "Vertex count and source/destination columns must be valid.");
        }
        Snapshot snapshot = table.snapshot(snapshotId);
        if (snapshot == null) {
            throw new IllegalArgumentException("Iceberg snapshot does not exist: " + snapshotId);
        }
        if (!directory(grapharRoot).equals(graphWriter.datasetRoot())) {
            throw new IllegalArgumentException(
                    "GraphAr root must match the supplied GraphWriter dataset root.");
        }
        IcebergFileIOStorage storage = new IcebergFileIOStorage(table.io());
        ParquetPhysicalReader parquet = new ParquetPhysicalReader(storage);
        TableScan scan =
                table.newScan().useSnapshot(snapshotId).select(sourceColumn, destinationColumn);
        List<TopologyEdge> edges = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                if (task.file().format() != FileFormat.PARQUET) {
                    throw new UnsupportedOperationException(
                            "Iceberg topology export supports Parquet data files only: "
                                    + task.file().path());
                }
                if (!task.deletes().isEmpty()) {
                    throw new UnsupportedOperationException(
                            "Iceberg topology export rejects delete files: " + task.file().path());
                }
                readFile(
                        parquet,
                        URI.create(task.file().path().toString()),
                        sourceColumn,
                        destinationColumn,
                        edges);
                if (edges.size() > maxInMemoryEdges) {
                    throw new IllegalArgumentException(
                            "Iceberg snapshot exceeds configured in-memory edge limit: "
                                    + maxInMemoryEdges);
                }
            }
        }
        edges.sort(
                Comparator.comparingLong(TopologyEdge::source)
                        .thenComparingLong(TopologyEdge::destination));
        long edgeCount = graphWriter.writeOrderedSourceTopology(edgeInfo, sourceVertexCount, edges);
        return new ExportResult(snapshotId, edgeCount, grapharRoot);
    }

    private static URI directory(URI uri) {
        String value = uri.toString();
        return URI.create(value.endsWith("/") ? value : value + "/");
    }

    /**
     * Publishes one explicitly selected Iceberg snapshot through the table's {@code FileIO}.
     * Provenance is written before the GraphAr root metadata publication marker.
     */
    public ExportResult exportAndPublish(
            Table table,
            long snapshotId,
            GraphInfo graphInfo,
            EdgeInfo edgeInfo,
            long sourceVertexCount,
            String sourceColumn,
            String destinationColumn,
            URI datasetRoot,
            URI graphYamlUri,
            WriteMode writeMode)
            throws IOException {
        Objects.requireNonNull(graphInfo, "Graph info cannot be null.");
        Objects.requireNonNull(datasetRoot, "Dataset root cannot be null.");
        Objects.requireNonNull(graphYamlUri, "Graph YAML URI cannot be null.");
        IcebergFileIOStorage storage = new IcebergFileIOStorage(table.io());
        GraphWriter writer =
                new GraphWriter(
                        graphInfo,
                        datasetRoot,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        Objects.requireNonNull(writeMode, "Write mode cannot be null."));
        ExportResult result =
                exportOrderedSource(
                        table,
                        snapshotId,
                        edgeInfo,
                        sourceVertexCount,
                        sourceColumn,
                        destinationColumn,
                        datasetRoot,
                        writer);
        writeProvenance(storage, datasetRoot.resolve("iceberg-export.yml"), result, writeMode);
        writer.writeMetadata(graphYamlUri);
        return result;
    }

    private static void readFile(
            ParquetPhysicalReader parquet,
            URI uri,
            String sourceColumn,
            String destinationColumn,
            List<TopologyEdge> edges)
            throws IOException {
        ReadResult result =
                parquet.read(
                        ReadRequest.builder(uri)
                                .projection(Projection.of(List.of(sourceColumn, destinationColumn)))
                                .build());
        try (BatchCursor cursor = result.cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                ValueVector sources = batch.column(0);
                ValueVector destinations = batch.column(1);
                for (int index = 0; index < batch.rowCount(); index++) {
                    Object source = sources.getObject(index);
                    Object destination = destinations.getObject(index);
                    if (!(source instanceof Long) || !(destination instanceof Long)) {
                        throw new IllegalArgumentException(
                                "Iceberg topology columns must be required INT64 values in " + uri);
                    }
                    edges.add(new TopologyEdge((Long) source, (Long) destination));
                }
            }
        }
    }

    private static void writeProvenance(
            IcebergFileIOStorage storage, URI uri, ExportResult result, WriteMode writeMode)
            throws IOException {
        String yaml =
                "iceberg_snapshot_id: "
                        + result.snapshotId()
                        + "\nedge_count: "
                        + result.edgeCount()
                        + "\ngraphar_root: "
                        + result.grapharRoot()
                        + "\n";
        try (PositionOutput output =
                writeMode == WriteMode.CREATE_NEW
                        ? storage.outputFile(uri).create()
                        : storage.outputFile(uri).createOrOverwrite()) {
            output.write(yaml.getBytes(StandardCharsets.UTF_8));
        }
    }
}
