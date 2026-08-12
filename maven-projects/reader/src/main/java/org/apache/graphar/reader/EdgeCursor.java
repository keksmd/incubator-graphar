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
import org.apache.graphar.core.ChunkMath;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RowRange;

/** A closeable sequential cursor over ordered-by-source GraphAr topology rows. */
public final class EdgeCursor implements AutoCloseable {
    private static final List<String> TOPOLOGY_COLUMNS =
            List.of("_graphArSrcIndex", "_graphArDstIndex");

    private final EdgeInfo edgeInfo;
    private final URI datasetRoot;
    private final PhysicalReader physicalReader;
    private final long[] edgeCounts;
    private final long vertexCount;
    private final boolean limited;
    private final List<ReadReport> reports = new ArrayList<>();
    private long remaining;
    private long partitionIndex;
    private long edgeChunkIndex;
    private long activePartitionIndex = -1;
    private long expectedRowsInChunk;
    private long observedRowsInChunk;
    private BatchCursor batchCursor;
    private RecordBatch batch;
    private int rowIndex;
    private Long source;
    private Long destination;
    private boolean closed;

    EdgeCursor(
            EdgeInfo edgeInfo,
            URI datasetRoot,
            PhysicalReader physicalReader,
            long[] edgeCounts,
            long vertexCount,
            long limit,
            boolean limited) {
        this.edgeInfo = Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        this.datasetRoot = DatasetUris.directory(datasetRoot);
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
        this.edgeCounts = edgeCounts.clone();
        if (vertexCount < 0) {
            throw new IllegalArgumentException("Vertex count must be non-negative: " + vertexCount);
        }
        this.vertexCount = vertexCount;
        if (limit < 0) {
            throw new IllegalArgumentException("Edge limit must be non-negative: " + limit);
        }
        this.remaining = limit;
        this.limited = limited;
    }

    /** Advances to the next edge. */
    public boolean next() throws IOException {
        source = null;
        destination = null;
        if (closed || remaining == 0) {
            close();
            return false;
        }
        while (true) {
            if (batch != null && rowIndex < batch.rowCount()) {
                Object sourceValue = batch.row(rowIndex).value(0);
                Object destinationValue = batch.row(rowIndex).value(1);
                rowIndex++;
                source = nonNegativeId(sourceValue, "source");
                destination = nonNegativeId(destinationValue, "destination");
                validateSourcePartition(source);
                if (++observedRowsInChunk > expectedRowsInChunk) {
                    throw new IllegalArgumentException(
                            "Topology chunk contains more rows than its GraphAr edge count.");
                }
                remaining--;
                return true;
            }
            batch = null;
            rowIndex = 0;
            if (batchCursor != null) {
                if (batchCursor.next()) {
                    batch = batchCursor.batch();
                    continue;
                }
                verifyChunkRows();
                closeBatchCursor();
            }
            if (!openNextEdgeChunk()) {
                close();
                return false;
            }
        }
    }

    /** Returns the current source ID after {@link #next()} returns {@code true}. */
    public long source() {
        return current(source, "source");
    }

    /** Returns the current destination ID after {@link #next()} returns {@code true}. */
    public long destination() {
        return current(destination, "destination");
    }

    /** Returns immutable physical capability reports in adjacency-request order. */
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
        source = null;
        destination = null;
    }

    private boolean openNextEdgeChunk() throws IOException {
        while (partitionIndex < edgeCounts.length) {
            long partitionEdgeCount = edgeCounts[(int) partitionIndex];
            long partitionChunkCount =
                    ChunkMath.chunkCount(partitionEdgeCount, edgeInfo.getChunkSize());
            if (edgeChunkIndex >= partitionChunkCount) {
                partitionIndex++;
                edgeChunkIndex = 0;
                continue;
            }
            long currentEdgeChunk = edgeChunkIndex++;
            long chunkStart = Math.multiplyExact(currentEdgeChunk, edgeInfo.getChunkSize());
            long chunkLength = Math.min(edgeInfo.getChunkSize(), partitionEdgeCount - chunkStart);
            ReadRequest.Builder request =
                    ReadRequest.builder(
                                    DatasetUris.resolve(
                                            datasetRoot,
                                            edgeInfo.getAdjacentListChunkUri(
                                                    AdjListType.ordered_by_source,
                                                    partitionIndex,
                                                    currentEdgeChunk)))
                            .projection(Projection.of(TOPOLOGY_COLUMNS))
                            .rowRange(new RowRange(0, chunkLength));
            if (limited) {
                request.limit(remaining);
            }
            ReadResult result = physicalReader.read(request.build());
            reports.add(result.report());
            batchCursor = result.cursor();
            activePartitionIndex = partitionIndex;
            expectedRowsInChunk = chunkLength;
            observedRowsInChunk = 0;
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

    private void validateSourcePartition(long sourceId) {
        long partitionStart = Math.multiplyExact(activePartitionIndex, edgeInfo.getSrcChunkSize());
        long partitionLength = Math.min(edgeInfo.getSrcChunkSize(), vertexCount - partitionStart);
        long partitionEnd = partitionStart + partitionLength;
        if (sourceId < partitionStart || sourceId >= partitionEnd) {
            throw new IllegalArgumentException(
                    "Topology source ID is outside its ordered-by-source partition: " + sourceId);
        }
    }

    private void verifyChunkRows() {
        if (observedRowsInChunk != expectedRowsInChunk) {
            throw new IllegalArgumentException(
                    "Topology chunk row count does not match its GraphAr edge count.");
        }
    }

    private static long nonNegativeId(Object value, String kind) {
        if (!(value instanceof Long) || ((Long) value) < 0) {
            throw new IllegalArgumentException(
                    "GraphAr " + kind + " IDs must be non-negative INT64 values.");
        }
        return (Long) value;
    }

    private static long current(Long value, String kind) {
        if (value == null) {
            throw new IllegalStateException(
                    "No current " + kind + " edge value. Call next() first.");
        }
        return value;
    }
}
