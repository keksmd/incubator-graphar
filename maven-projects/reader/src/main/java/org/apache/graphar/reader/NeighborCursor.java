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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.graphar.core.ResolvedAdjacency;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RowRange;

/** A closeable cursor over destination IDs selected by one ordered-by-source adjacency range. */
public final class NeighborCursor implements AutoCloseable {
    static final String DESTINATION_COLUMN = "_graphArDstIndex";

    private final PhysicalReader physicalReader;
    private final ResolvedAdjacency resolved;
    private final URI datasetRoot;
    private final long edgeChunkSize;
    private final boolean limited;
    private final List<ReadReport> reports;
    private long nextEdgeChunk;
    private long remaining;
    private BatchCursor batchCursor;
    private RecordBatch batch;
    private int rowIndex;
    private Long current;
    private boolean closed;

    NeighborCursor(
            PhysicalReader physicalReader,
            ResolvedAdjacency resolved,
            URI datasetRoot,
            long edgeChunkSize,
            long limit,
            boolean limited,
            ReadReport offsetReport) {
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
        this.resolved = Objects.requireNonNull(resolved, "Resolved adjacency cannot be null.");
        this.datasetRoot = Objects.requireNonNull(datasetRoot, "Dataset root cannot be null.");
        if (edgeChunkSize <= 0) {
            throw new IllegalArgumentException(
                    "Edge chunk size must be positive: " + edgeChunkSize);
        }
        if (limit < 0) {
            throw new IllegalArgumentException("Neighbor limit must be non-negative: " + limit);
        }
        this.edgeChunkSize = edgeChunkSize;
        this.limited = limited;
        this.reports = new ArrayList<>();
        this.reports.add(
                Objects.requireNonNull(offsetReport, "Offset read report cannot be null."));
        this.nextEdgeChunk = resolved.edgeChunks().begin();
        this.remaining = limit;
    }

    /** Advances to the next destination ID. */
    public boolean next() throws IOException {
        current = null;
        if (closed || remaining == 0) {
            close();
            return false;
        }
        while (true) {
            if (batch != null && rowIndex < batch.rowCount()) {
                Object value = batch.row(rowIndex++).value(0);
                if (!(value instanceof Long) || ((Long) value) < 0) {
                    throw new IllegalArgumentException(
                            "GraphAr destination IDs must be non-negative INT64 values.");
                }
                current = (Long) value;
                remaining--;
                return true;
            }
            batch = null;
            rowIndex = 0;
            if (batchCursor != null) {
                if (batchCursor.next()) {
                    batch = batchCursor.batch();
                    if (batch.rowCount() == 0) {
                        batch = null;
                    }
                    continue;
                }
                closeBatchCursor();
            }
            if (!openNextEdgeChunk()) {
                close();
                return false;
            }
        }
    }

    /** Returns the current destination ID after {@link #next()} returns {@code true}. */
    public long destination() {
        if (current == null) {
            throw new IllegalStateException("No current neighbor. Call next() first.");
        }
        return current;
    }

    /**
     * Returns immutable physical capability reports in request order, including the offset read.
     */
    public List<ReadReport> reports() {
        return List.copyOf(reports);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            closeBatchCursor();
        }
        batch = null;
        current = null;
    }

    private boolean openNextEdgeChunk() throws IOException {
        while (nextEdgeChunk < resolved.edgeChunks().end()) {
            long edgeChunkIndex = nextEdgeChunk++;
            long chunkStart = Math.multiplyExact(edgeChunkIndex, edgeChunkSize);
            long chunkEnd =
                    chunkStart > Long.MAX_VALUE - edgeChunkSize
                            ? Long.MAX_VALUE
                            : chunkStart + edgeChunkSize;
            long rangeStart = Math.max(resolved.edgeRange().begin(), chunkStart);
            long rangeEnd = Math.min(resolved.edgeRange().end(), chunkEnd);
            if (rangeStart == rangeEnd) {
                continue;
            }
            ReadRequest.Builder request =
                    ReadRequest.builder(resolveUri(resolved.adjacencyChunkUri(edgeChunkIndex)))
                            .projection(Projection.of(List.of(DESTINATION_COLUMN)))
                            .rowRange(new RowRange(rangeStart - chunkStart, rangeEnd - chunkStart));
            if (limited) {
                request.limit(remaining);
            }
            ReadResult result = physicalReader.read(request.build());
            reports.add(result.report());
            batchCursor = result.cursor();
            return true;
        }
        return false;
    }

    private void closeBatchCursor() throws IOException {
        if (batchCursor != null) {
            BatchCursor cursor = batchCursor;
            batchCursor = null;
            cursor.close();
        }
    }

    private URI resolveUri(URI uri) {
        return uri.isAbsolute() ? uri : datasetRoot.resolve(uri);
    }
}
