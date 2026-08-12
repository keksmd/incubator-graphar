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
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.storage.Storage;

/** Reads one declared GraphAr adjacency layout, including aligned edge property groups. */
public final class EdgeLayoutReader {
    private static final String OFFSET_COLUMN = "_graphArOffset";

    private final EdgeInfo edgeInfo;
    private final AdjListType layout;
    private final URI datasetRoot;
    private final Storage storage;
    private final PhysicalReader physicalReader;

    EdgeLayoutReader(
            EdgeInfo edgeInfo,
            AdjListType layout,
            URI datasetRoot,
            Storage storage,
            PhysicalReader physicalReader) {
        this.edgeInfo = Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        this.layout = Objects.requireNonNull(layout, "Adjacency layout cannot be null.");
        if (!edgeInfo.hasAdjListType(layout)) {
            throw new IllegalArgumentException(
                    "Edge info does not declare adjacency layout: " + layout);
        }
        this.datasetRoot = DatasetUris.directory(datasetRoot);
        this.storage = Objects.requireNonNull(storage, "Storage cannot be null.");
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
    }

    /** Returns the declared number of vertices for this layout's aligned endpoint. */
    public long vertexCount() throws IOException {
        return ControlFileReader.readNonNegativeLong(
                storage, DatasetUris.resolve(datasetRoot, edgeInfo.getVerticesNumFileUri(layout)));
    }

    /** Returns the sum of partition edge counts. */
    public long edgeCount() throws IOException {
        long total = 0;
        for (long count : partitionEdgeCounts(vertexCount())) total = Math.addExact(total, count);
        return total;
    }

    /** Opens a cursor over every topology row and its property values. */
    public EdgePropertyCursor scanEdges() throws IOException {
        long vertexCount = vertexCount();
        long[] counts = partitionEdgeCounts(vertexCount);
        List<EdgePropertyCursor.Segment> segments = new ArrayList<>();
        for (int partition = 0; partition < counts.length; partition++) {
            addRange(segments, partition, 0, counts[partition]);
        }
        return new EdgePropertyCursor(
                edgeInfo, layout, datasetRoot, physicalReader, segments, null);
    }

    /**
     * Opens a cursor for one source or destination vertex, according to this layout's alignment.
     * Ordered layouts use their offset index; unordered layouts scan only that aligned partition.
     */
    public EdgePropertyCursor edges(long alignedVertexId) throws IOException {
        long vertexCount = vertexCount();
        if (alignedVertexId < 0 || alignedVertexId >= vertexCount) {
            throw new IllegalArgumentException(
                    "Aligned vertex ID is outside this adjacency layout.");
        }
        long chunkSize = vertexChunkSize();
        long partition = alignedVertexId / chunkSize;
        long[] counts = partitionEdgeCounts(vertexCount);
        List<EdgePropertyCursor.Segment> segments = new ArrayList<>();
        if (layout.isOrdered()) {
            long local = alignedVertexId % chunkSize;
            long[] range = readOffsetPair(partition, local);
            if (range[1] < range[0] || range[1] > counts[(int) partition]) {
                throw new IllegalArgumentException(
                        "GraphAr offset range is outside its partition edge count.");
            }
            addRange(segments, partition, range[0], range[1]);
            return new EdgePropertyCursor(
                    edgeInfo, layout, datasetRoot, physicalReader, segments, null);
        }
        addRange(segments, partition, 0, counts[(int) partition]);
        return new EdgePropertyCursor(
                edgeInfo, layout, datasetRoot, physicalReader, segments, alignedVertexId);
    }

    private long[] partitionEdgeCounts(long vertexCount) throws IOException {
        long partitionCount = ChunkMath.chunkCount(vertexCount, vertexChunkSize());
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
                                    datasetRoot, edgeInfo.getEdgesNumFileUri(layout, partition)));
        }
        return counts;
    }

    private long[] readOffsetPair(long partition, long localVertex) throws IOException {
        ReadResult result =
                physicalReader.read(
                        ReadRequest.builder(
                                        DatasetUris.resolve(
                                                datasetRoot,
                                                edgeInfo.getOffsetChunkUri(layout, partition)))
                                .projection(Projection.of(List.of(OFFSET_COLUMN)))
                                .rowRange(new RowRange(localVertex, localVertex + 2))
                                .build());
        List<Long> values = new ArrayList<>(2);
        try (BatchCursor cursor = result.cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                for (int row = 0; row < batch.rowCount(); row++) {
                    Object value = batch.row(row).value(0);
                    if (!(value instanceof Long) || (Long) value < 0) {
                        throw new IllegalArgumentException(
                                "GraphAr offsets must be non-negative INT64 values.");
                    }
                    values.add((Long) value);
                }
            }
        }
        if (values.size() != 2) {
            throw new IllegalArgumentException(
                    "GraphAr ordered adjacency needs exactly two offsets.");
        }
        return new long[] {values.get(0), values.get(1)};
    }

    private void addRange(
            List<EdgePropertyCursor.Segment> segments, long partition, long begin, long end) {
        long chunkSize = edgeInfo.getChunkSize();
        for (long edgeChunk = begin / chunkSize;
                edgeChunk < ChunkMath.chunkCount(end, chunkSize);
                edgeChunk++) {
            long chunkStart = Math.multiplyExact(edgeChunk, chunkSize);
            long rangeStart = Math.max(begin, chunkStart);
            long rangeEnd = Math.min(end, chunkStart + chunkSize);
            if (rangeStart < rangeEnd) {
                segments.add(
                        new EdgePropertyCursor.Segment(
                                partition,
                                edgeChunk,
                                new RowRange(rangeStart - chunkStart, rangeEnd - chunkStart)));
            }
        }
    }

    private long vertexChunkSize() {
        return layout.getAlignedBy().equals("src")
                ? edgeInfo.getSrcChunkSize()
                : edgeInfo.getDstChunkSize();
    }
}
