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
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.storage.s3.S3Storage;
import org.apache.graphar.writer.GraphWriter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import software.amazon.awssdk.services.s3.S3Client;

/** Publishes a snapshot-pinned ordered-source GraphAr topology to an explicit S3 target. */
public final class S3IcebergTopologyExport {
    private final IcebergTopologyExporter topologyExporter;

    /** Creates an exporter with a strict upper bound on its source-sorting edge index. */
    public S3IcebergTopologyExport(long maxInMemoryEdges) {
        this.topologyExporter = new IcebergTopologyExporter(maxInMemoryEdges);
    }

    /**
     * Reads exactly {@code snapshotId} through the table's FileIO and publishes data, manifest,
     * metadata references, and graph YAML to the injected S3 client in that order.
     */
    public IcebergTopologyExporter.ExportResult exportOrderedSource(
            Table table,
            long snapshotId,
            GraphInfo graphInfo,
            EdgeInfo edgeInfo,
            long sourceVertexCount,
            String sourceColumn,
            String destinationColumn,
            URI grapharRoot,
            URI graphYamlUri,
            URI manifestUri,
            S3Client s3Client,
            Path stagingDirectory,
            WriteMode writeMode)
            throws IOException {
        Objects.requireNonNull(table, "Iceberg table cannot be null.");
        Objects.requireNonNull(graphInfo, "Graph info cannot be null.");
        Snapshot snapshot = table.snapshot(snapshotId);
        if (snapshot == null) {
            throw new IllegalArgumentException("Iceberg snapshot does not exist: " + snapshotId);
        }
        requireS3(grapharRoot, "GraphAr root");
        requireS3(graphYamlUri, "Graph YAML URI");
        requireS3(manifestUri, "Export manifest URI");
        S3Storage storage = new S3Storage(s3Client, stagingDirectory);
        GraphInfo publishedGraphInfo =
                graphInfo.withExtraInfo(
                        Map.of(
                                "graphar.export.manifest", manifestUri.toString(),
                                "iceberg.snapshot.id", Long.toString(snapshotId),
                                "iceberg.table.uuid", table.uuid().toString()));
        GraphWriter graphWriter =
                new GraphWriter(
                        publishedGraphInfo,
                        grapharRoot,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        writeMode);
        IcebergTopologyExporter.ExportResult result =
                topologyExporter.exportOrderedSource(
                        table,
                        snapshotId,
                        edgeInfo,
                        sourceVertexCount,
                        sourceColumn,
                        destinationColumn,
                        grapharRoot,
                        graphWriter);
        new ExportManifest(
                        table.uuid().toString(),
                        snapshotId,
                        snapshot.manifestListLocation(),
                        sourceColumn,
                        destinationColumn,
                        result.grapharRoot(),
                        result.edgeCount())
                .write(storage, manifestUri, writeMode);
        graphWriter.writeMetadata(graphYamlUri);
        return result;
    }

    private static void requireS3(URI uri, String kind) {
        if (uri == null || !"s3".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException(kind + " must be an absolute s3:// URI: " + uri);
        }
    }
}
