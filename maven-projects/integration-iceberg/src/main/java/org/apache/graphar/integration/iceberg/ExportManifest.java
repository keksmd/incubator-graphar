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
import java.util.Objects;
import java.util.Properties;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.storage.PositionOutput;
import org.apache.graphar.storage.Storage;

/** Durable provenance for one snapshot-pinned Iceberg-to-GraphAr export. */
public final class ExportManifest {
    private final String tableUuid;
    private final long snapshotId;
    private final String snapshotManifestLocation;
    private final String sourceColumn;
    private final String destinationColumn;
    private final URI grapharRoot;
    private final long edgeCount;

    public ExportManifest(
            String tableUuid,
            long snapshotId,
            String snapshotManifestLocation,
            String sourceColumn,
            String destinationColumn,
            URI grapharRoot,
            long edgeCount) {
        this.tableUuid = Objects.requireNonNull(tableUuid, "Iceberg table UUID cannot be null.");
        this.snapshotId = snapshotId;
        this.snapshotManifestLocation =
                Objects.requireNonNull(
                        snapshotManifestLocation, "Snapshot manifest location cannot be null.");
        this.sourceColumn = Objects.requireNonNull(sourceColumn, "Source column cannot be null.");
        this.destinationColumn =
                Objects.requireNonNull(destinationColumn, "Destination column cannot be null.");
        this.grapharRoot = Objects.requireNonNull(grapharRoot, "GraphAr root cannot be null.");
        this.edgeCount = edgeCount;
    }

    /** Writes stable Java properties before the GraphAr root YAML is published. */
    public void write(Storage storage, URI uri, WriteMode mode) throws IOException {
        Objects.requireNonNull(storage, "Storage cannot be null.");
        Objects.requireNonNull(uri, "Manifest URI cannot be null.");
        Properties values = new Properties();
        values.setProperty("format_version", "1");
        values.setProperty("iceberg_table_uuid", tableUuid);
        values.setProperty("iceberg_snapshot_id", Long.toString(snapshotId));
        values.setProperty("iceberg_snapshot_manifest", snapshotManifestLocation);
        values.setProperty("source_column", sourceColumn);
        values.setProperty("destination_column", destinationColumn);
        values.setProperty("graphar_root", grapharRoot.toString());
        values.setProperty("edge_count", Long.toString(edgeCount));
        StringBuilder encoded = new StringBuilder();
        for (String name : values.stringPropertyNames().stream().sorted().toArray(String[]::new)) {
            encoded.append(name).append('=').append(values.getProperty(name)).append('\n');
        }
        try (PositionOutput output =
                mode == WriteMode.CREATE_NEW
                        ? storage.outputFile(uri).create()
                        : storage.outputFile(uri).createOrOverwrite()) {
            output.write(encoded.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}
