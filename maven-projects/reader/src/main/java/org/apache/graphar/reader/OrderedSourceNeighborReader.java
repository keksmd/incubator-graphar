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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.graphar.core.EdgeRange;
import org.apache.graphar.core.OffsetChunk;
import org.apache.graphar.core.OffsetLocation;
import org.apache.graphar.core.OrderedAdjacencyResolver;
import org.apache.graphar.core.ResolvedAdjacency;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;

/** Reads destination IDs from GraphAr {@code ordered_by_source} adjacency layouts. */
public final class OrderedSourceNeighborReader {
    private static final String OFFSET_COLUMN = "_graphArOffset";

    private final EdgeInfo edgeInfo;
    private final URI datasetRoot;
    private final PhysicalReader physicalReader;
    private final OrderedAdjacencyResolver resolver;
    private final ConcurrentMap<URI, OffsetChunk> offsetChunks = new ConcurrentHashMap<>();

    /**
     * Creates a reader whose relative GraphAr paths resolve below {@code datasetRoot}. The root
     * identifies a directory and is normalized to include a trailing slash.
     */
    public OrderedSourceNeighborReader(
            EdgeInfo edgeInfo, URI datasetRoot, PhysicalReader physicalReader) {
        this.edgeInfo = Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        this.datasetRoot = DatasetUris.directory(datasetRoot);
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
        this.resolver = new OrderedAdjacencyResolver(edgeInfo, AdjListType.ordered_by_source);
    }

    /** Opens a cursor over every destination adjacent to {@code sourceVertexId}. */
    public NeighborCursor neighbors(long sourceVertexId) throws IOException {
        return openNeighbors(sourceVertexId, Long.MAX_VALUE, false);
    }

    /**
     * Opens a cursor over at most {@code limit} destinations adjacent to {@code sourceVertexId}.
     */
    public NeighborCursor neighbors(long sourceVertexId, long limit) throws IOException {
        if (limit < 0) {
            throw new IllegalArgumentException("Neighbor limit must be non-negative: " + limit);
        }
        return openNeighbors(sourceVertexId, limit, true);
    }

    private NeighborCursor openNeighbors(long sourceVertexId, long limit, boolean limited)
            throws IOException {
        OffsetLocation location = resolver.locate(sourceVertexId);
        URI offsetUri = DatasetUris.resolve(datasetRoot, location.offsetChunkUri());
        LoadedOffsetChunk loaded = loadOffsetChunk(offsetUri);
        EdgeRange offsets = loaded.offsetChunk.rangeFor(location.offsetIndex());
        ResolvedAdjacency resolved =
                resolver.resolve(sourceVertexId, offsets.begin(), offsets.end());
        return new NeighborCursor(
                physicalReader,
                resolved,
                datasetRoot,
                edgeInfo.getChunkSize(),
                limit,
                limited,
                loaded.report);
    }

    private LoadedOffsetChunk loadOffsetChunk(URI offsetUri) throws IOException {
        OffsetChunk cached = offsetChunks.get(offsetUri);
        if (cached != null) {
            return new LoadedOffsetChunk(cached, null);
        }
        synchronized (offsetChunks) {
            cached = offsetChunks.get(offsetUri);
            if (cached != null) {
                return new LoadedOffsetChunk(cached, null);
            }
            ReadRequest request =
                    ReadRequest.builder(offsetUri)
                            .projection(Projection.of(List.of(OFFSET_COLUMN)))
                            .build();
            ReadResult result = physicalReader.read(request);
            OffsetChunk loaded = OffsetChunk.of(readOffsets(result.cursor()));
            offsetChunks.put(offsetUri, loaded);
            return new LoadedOffsetChunk(loaded, result.report());
        }
    }

    private static long[] readOffsets(BatchCursor cursor) throws IOException {
        List<Long> offsets = new ArrayList<>();
        try (BatchCursor closeableCursor = cursor) {
            while (closeableCursor.next()) {
                RecordBatch batch = closeableCursor.batch();
                for (int index = 0; index < batch.rowCount(); index++) {
                    Object value = batch.row(index).value(0);
                    if (!(value instanceof Long) || ((Long) value) < 0) {
                        throw new IllegalArgumentException(
                                "GraphAr offsets must be non-negative INT64 values.");
                    }
                    offsets.add((Long) value);
                }
            }
        }
        long[] values = new long[offsets.size()];
        for (int index = 0; index < values.length; index++) {
            values[index] = offsets.get(index);
        }
        return values;
    }

    private static final class LoadedOffsetChunk {
        private final OffsetChunk offsetChunk;
        private final org.apache.graphar.io.ReadReport report;

        private LoadedOffsetChunk(
                OffsetChunk offsetChunk, org.apache.graphar.io.ReadReport report) {
            this.offsetChunk = offsetChunk;
            this.report = report;
        }
    }
}
