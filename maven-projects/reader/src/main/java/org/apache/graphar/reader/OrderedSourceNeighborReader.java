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
import org.apache.graphar.io.RowRange;

/** Reads destination IDs from GraphAr {@code ordered_by_source} adjacency layouts. */
public final class OrderedSourceNeighborReader {
    private static final String OFFSET_COLUMN = "_graphArOffset";

    private final EdgeInfo edgeInfo;
    private final URI datasetRoot;
    private final PhysicalReader physicalReader;
    private final OrderedAdjacencyResolver resolver;

    /**
     * Creates a reader whose relative GraphAr paths resolve below {@code datasetRoot}. The root
     * identifies a directory and is normalized to include a trailing slash.
     */
    public OrderedSourceNeighborReader(
            EdgeInfo edgeInfo, URI datasetRoot, PhysicalReader physicalReader) {
        this.edgeInfo = Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        this.datasetRoot = directoryUri(datasetRoot);
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
        long offsetEnd = Math.addExact(location.offsetIndex(), 2);
        ReadRequest request =
                ReadRequest.builder(resolveUri(location.offsetChunkUri()))
                        .projection(Projection.of(List.of(OFFSET_COLUMN)))
                        .rowRange(new RowRange(location.offsetIndex(), offsetEnd))
                        .build();
        ReadResult result = physicalReader.read(request);
        List<Long> offsets = readOffsets(result.cursor());
        if (offsets.size() != 2) {
            throw new IllegalArgumentException(
                    "Expected two offsets for source vertex "
                            + sourceVertexId
                            + " but read "
                            + offsets.size()
                            + ".");
        }
        ResolvedAdjacency resolved =
                resolver.resolve(sourceVertexId, offsets.get(0), offsets.get(1));
        return new NeighborCursor(
                physicalReader,
                resolved,
                datasetRoot,
                edgeInfo.getChunkSize(),
                limit,
                limited,
                result.report());
    }

    private static List<Long> readOffsets(BatchCursor cursor) throws IOException {
        List<Long> offsets = new ArrayList<>(2);
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
        return offsets;
    }

    private URI resolveUri(URI uri) {
        return uri.isAbsolute() ? uri : datasetRoot.resolve(uri);
    }

    private static URI directoryUri(URI datasetRoot) {
        Objects.requireNonNull(datasetRoot, "Dataset root cannot be null.");
        String value = datasetRoot.toString();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Dataset root cannot be empty.");
        }
        return value.endsWith("/") ? datasetRoot : URI.create(value + "/");
    }
}
