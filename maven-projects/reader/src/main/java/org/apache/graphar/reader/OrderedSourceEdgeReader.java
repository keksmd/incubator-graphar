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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.graphar.core.ChunkMath;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.storage.Storage;

/** Reads the topology of one GraphAr edge type from its ordered-by-source representation. */
public final class OrderedSourceEdgeReader {
    private final EdgeInfo edgeInfo;
    private final URI datasetRoot;
    private final Storage storage;
    private final OrderedSourceNeighborReader neighborReader;
    private final PhysicalReader physicalReader;

    /** Creates an edge reader using a storage backend for GraphAr control files. */
    public OrderedSourceEdgeReader(
            EdgeInfo edgeInfo, URI datasetRoot, Storage storage, PhysicalReader physicalReader) {
        this.edgeInfo = Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        if (!edgeInfo.hasAdjListType(AdjListType.ordered_by_source)) {
            throw new IllegalArgumentException(
                    "Edge info does not contain ordered_by_source adjacency: "
                            + edgeInfo.getConcat());
        }
        this.datasetRoot = DatasetUris.directory(datasetRoot);
        this.storage = Objects.requireNonNull(storage, "Storage cannot be null.");
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
        this.neighborReader =
                new OrderedSourceNeighborReader(edgeInfo, this.datasetRoot, physicalReader);
    }

    /** Opens a cursor over every destination adjacent to {@code sourceVertexId}. */
    public NeighborCursor neighbors(long sourceVertexId) throws IOException {
        return neighborReader.neighbors(sourceVertexId);
    }

    /**
     * Opens a cursor over at most {@code limit} destinations adjacent to {@code sourceVertexId}.
     */
    public NeighborCursor neighbors(long sourceVertexId, long limit) throws IOException {
        return neighborReader.neighbors(sourceVertexId, limit);
    }

    /** Reads a bounded frontier in physical adjacency-chunk batches. */
    public Map<Long, List<Long>> neighbors(Collection<Long> sourceVertexIds) throws IOException {
        return neighborReader.neighbors(sourceVertexIds);
    }

    /** Returns the number of source vertices represented by this adjacency layout. */
    public long vertexCount() throws IOException {
        return ControlFileReader.readNonNegativeLong(
                storage,
                DatasetUris.resolve(
                        datasetRoot,
                        edgeInfo.getVerticesNumFileUri(AdjListType.ordered_by_source)));
    }

    /** Returns the total number of edges represented by this adjacency layout. */
    public long edgeCount() throws IOException {
        long total = 0;
        for (long count : partitionEdgeCounts(vertexCount())) {
            total = Math.addExact(total, count);
        }
        return total;
    }

    /** Opens a sequential cursor over every source/destination topology row. */
    public EdgeCursor scanEdges() throws IOException {
        return openEdgeScan(Long.MAX_VALUE, false);
    }

    /** Opens a sequential cursor over at most {@code limit} source/destination topology rows. */
    public EdgeCursor scanEdges(long limit) throws IOException {
        if (limit < 0) {
            throw new IllegalArgumentException("Edge limit must be non-negative: " + limit);
        }
        return openEdgeScan(limit, true);
    }

    /** Materializes the outgoing adjacency of this topology as a bounded heap CSR. */
    public CsrGraph materializeCsr(long maxVertices, long maxEdges) throws IOException {
        return materializeCsr(maxVertices, maxEdges, CsrDirection.OUTGOING);
    }

    /**
     * Materializes the requested adjacency of this topology as a bounded heap CSR. Reversing
     * directions transpose the stored edges in memory instead of requiring a second {@code
     * ordered_by_dest} projection on disk.
     */
    public CsrGraph materializeCsr(long maxVertices, long maxEdges, CsrDirection direction)
            throws IOException {
        Objects.requireNonNull(direction, "CSR direction cannot be null.");
        return CsrMaterializer.materialize(this, edgeInfo, maxVertices, maxEdges, direction);
    }

    private EdgeCursor openEdgeScan(long limit, boolean limited) throws IOException {
        long vertexCount = vertexCount();
        return new EdgeCursor(
                edgeInfo,
                datasetRoot,
                physicalReader,
                partitionEdgeCounts(vertexCount),
                vertexCount,
                limit,
                limited);
    }

    long[] partitionEdgeCounts(long vertexCount) throws IOException {
        long partitionCount = ChunkMath.chunkCount(vertexCount, edgeInfo.getSrcChunkSize());
        if (partitionCount > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Too many edge partitions for a Java array: " + partitionCount);
        }
        long[] counts = new long[(int) partitionCount];
        for (int partition = 0; partition < counts.length; partition++) {
            counts[partition] =
                    ControlFileReader.readNonNegativeLong(
                            storage,
                            DatasetUris.resolve(
                                    datasetRoot,
                                    edgeInfo.getEdgesNumFileUri(
                                            AdjListType.ordered_by_source, partition)));
        }
        return counts;
    }
}
