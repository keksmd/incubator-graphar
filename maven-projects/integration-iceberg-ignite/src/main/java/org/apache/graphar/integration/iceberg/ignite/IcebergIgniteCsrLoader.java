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

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import org.apache.graphar.integration.iceberg.IcebergFileIOStorage;
import org.apache.graphar.integration.ignite.IgniteCsrStore;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.ValueVector;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Streams a pinned, globally source-ordered Iceberg Parquet snapshot directly into Ignite CSR.
 *
 * <p>Iceberg table sort metadata is not accepted as proof that {@link TableScan#planFiles()} has a
 * globally ordered result. Callers must explicitly arrange the selected snapshot's data files and
 * rows in source order. This loader validates that invariant while streaming and rejects the
 * snapshot before it publishes an Ignite manifest if the order is broken.
 */
public final class IcebergIgniteCsrLoader {
    /** Immutable provenance and CSR size for one successful pinned load. */
    public static final class LoadResult {
        private final long icebergSnapshotId;
        private final IgniteCsrStore.LoadResult csr;

        private LoadResult(long icebergSnapshotId, IgniteCsrStore.LoadResult csr) {
            this.icebergSnapshotId = icebergSnapshotId;
            this.csr = csr;
        }

        public long icebergSnapshotId() {
            return icebergSnapshotId;
        }

        public IgniteCsrStore.LoadResult csr() {
            return csr;
        }
    }

    /**
     * Loads exactly {@code snapshotId}; the Ignite revision ID is derived from it unless supplied
     * by the caller as a separate immutable release label.
     */
    public LoadResult load(
            Table table,
            long snapshotId,
            long sourceVertexCount,
            String sourceColumn,
            String destinationColumn,
            IgniteCsrStore csr)
            throws IOException {
        return load(
                table,
                snapshotId,
                "iceberg-" + snapshotId,
                sourceVertexCount,
                sourceColumn,
                destinationColumn,
                csr);
    }

    /**
     * Loads one pinned Iceberg snapshot into a caller-owned Ignite store without collecting input
     * rows in heap.
     */
    public LoadResult load(
            Table table,
            long snapshotId,
            String csrSnapshotId,
            long sourceVertexCount,
            String sourceColumn,
            String destinationColumn,
            IgniteCsrStore csr)
            throws IOException {
        Objects.requireNonNull(table, "Iceberg table cannot be null.");
        Objects.requireNonNull(csr, "Ignite CSR store cannot be null.");
        requireColumn(sourceColumn, "Source column");
        requireColumn(destinationColumn, "Destination column");
        if (sourceVertexCount < 0) {
            throw new IllegalArgumentException("Source vertex count must be non-negative.");
        }
        Snapshot snapshot = table.snapshot(snapshotId);
        if (snapshot == null) {
            throw new IllegalArgumentException("Iceberg snapshot does not exist: " + snapshotId);
        }
        IcebergFileIOStorage storage = new IcebergFileIOStorage(table.io());
        ParquetPhysicalReader parquet = new ParquetPhysicalReader(storage);
        TableScan scan =
                table.newScan().useSnapshot(snapshotId).select(sourceColumn, destinationColumn);
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            return new LoadResult(
                    snapshotId,
                    csr.load(
                            csrSnapshotId,
                            sourceVertexCount,
                            new IcebergTopologyCursor(
                                    tasks, parquet, sourceColumn, destinationColumn)));
        }
    }

    private static void requireColumn(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank.");
        }
    }

    private static final class IcebergTopologyCursor implements IgniteCsrStore.TopologyCursor {
        private final java.util.Iterator<FileScanTask> tasks;
        private final ParquetPhysicalReader parquet;
        private final String sourceColumn;
        private final String destinationColumn;
        private BatchCursor batches;
        private RecordBatch batch;
        private ValueVector sources;
        private ValueVector destinations;
        private int rowIndex;
        private Long source;
        private Long destination;

        private IcebergTopologyCursor(
                Iterable<FileScanTask> tasks,
                ParquetPhysicalReader parquet,
                String sourceColumn,
                String destinationColumn) {
            this.tasks = tasks.iterator();
            this.parquet = parquet;
            this.sourceColumn = sourceColumn;
            this.destinationColumn = destinationColumn;
        }

        @Override
        public boolean next() throws IOException {
            source = null;
            destination = null;
            while (true) {
                if (batch != null && rowIndex < batch.rowCount()) {
                    source = requiredLong(sources.getObject(rowIndex), sourceColumn);
                    destination = requiredLong(destinations.getObject(rowIndex), destinationColumn);
                    rowIndex++;
                    return true;
                }
                batch = null;
                rowIndex = 0;
                if (batches != null && batches.next()) {
                    batch = batches.batch();
                    sources = batch.column(0);
                    destinations = batch.column(1);
                    continue;
                }
                closeBatches();
                if (!tasks.hasNext()) {
                    return false;
                }
                openTask(tasks.next());
            }
        }

        @Override
        public long source() {
            return current(source, sourceColumn);
        }

        @Override
        public long destination() {
            return current(destination, destinationColumn);
        }

        @Override
        public void close() throws IOException {
            closeBatches();
            batch = null;
            sources = null;
            destinations = null;
            source = null;
            destination = null;
        }

        private void openTask(FileScanTask task) throws IOException {
            if (task.file().format() != FileFormat.PARQUET) {
                throw new UnsupportedOperationException(
                        "Iceberg Ignite CSR loader supports Parquet data files only: "
                                + task.file().path());
            }
            if (!task.deletes().isEmpty()) {
                throw new UnsupportedOperationException(
                        "Iceberg Ignite CSR loader rejects delete files: " + task.file().path());
            }
            if (task.start() != 0 || task.length() != task.file().fileSizeInBytes()) {
                throw new UnsupportedOperationException(
                        "Iceberg Ignite CSR loader rejects byte-split files: "
                                + task.file().path());
            }
            ReadResult result =
                    parquet.read(
                            ReadRequest.builder(URI.create(task.file().path().toString()))
                                    .projection(
                                            Projection.of(List.of(sourceColumn, destinationColumn)))
                                    .build());
            batches = result.cursor();
        }

        private void closeBatches() throws IOException {
            if (batches != null) {
                batches.close();
                batches = null;
            }
            sources = null;
            destinations = null;
        }

        private static long requiredLong(Object value, String column) {
            if (!(value instanceof Long) || (Long) value < 0) {
                throw new IllegalArgumentException(
                        "Iceberg topology column must be a required non-negative INT64: " + column);
            }
            return (Long) value;
        }

        private static long current(Long value, String column) {
            if (value == null) {
                throw new IllegalStateException("No current " + column + ". Call next() first.");
            }
            return value;
        }
    }
}
