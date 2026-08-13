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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import org.apache.graphar.core.ChunkMath;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.storage.Storage;

/**
 * Reads one declared GraphAr vertex type. Property groups are joined by physical row position, and
 * selected vertices are translated into the smallest set of chunk row ranges that covers them.
 */
public final class VertexReader {
    private final VertexInfo vertexInfo;
    private final URI datasetRoot;
    private final Storage storage;
    private final PhysicalReader physicalReader;

    VertexReader(
            VertexInfo vertexInfo,
            URI datasetRoot,
            Storage storage,
            PhysicalReader physicalReader) {
        this.vertexInfo = Objects.requireNonNull(vertexInfo, "Vertex info cannot be null.");
        this.datasetRoot = DatasetUris.directory(datasetRoot);
        this.storage = Objects.requireNonNull(storage, "Storage cannot be null.");
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
    }

    /** Returns the immutable metadata that defines this vertex type. */
    public VertexInfo vertexInfo() {
        return vertexInfo;
    }

    /** Returns the declared number of vertices of this type. */
    public long vertexCount() throws IOException {
        return ControlFileReader.readNonNegativeLong(
                storage, DatasetUris.resolve(datasetRoot, vertexInfo.getVerticesNumFileUri()));
    }

    /** Reads every declared property of one vertex. */
    public Map<String, Object> properties(long vertexId) throws IOException {
        return properties(vertexId, VertexPropertyCursor.allPropertyNames(vertexInfo));
    }

    /** Reads only the requested properties of one vertex. */
    public Map<String, Object> properties(long vertexId, Collection<String> properties)
            throws IOException {
        Map<Long, Map<String, Object>> read = properties(List.of(vertexId), properties);
        Map<String, Object> values = read.get(vertexId);
        if (values == null) {
            throw new IllegalArgumentException(
                    "GraphAr vertex chunk does not contain vertex " + vertexId + '.');
        }
        return values;
    }

    /**
     * Reads the requested properties of several vertices. Consecutive vertices inside one chunk are
     * requested as a single physical row range, so a batch read never degrades into one request per
     * vertex.
     */
    public Map<Long, Map<String, Object>> properties(
            Collection<Long> vertexIds, Collection<String> properties) throws IOException {
        Objects.requireNonNull(vertexIds, "Vertex IDs cannot be null.");
        long vertexCount = vertexCount();
        TreeSet<Long> selected = new TreeSet<>();
        for (Long vertexId : vertexIds) {
            Objects.requireNonNull(vertexId, "Vertex IDs cannot contain null.");
            requireVertexId(vertexId, vertexCount);
            selected.add(vertexId);
        }
        Map<Long, Map<String, Object>> result = new LinkedHashMap<>();
        if (selected.isEmpty()) {
            return result;
        }
        try (VertexPropertyCursor cursor =
                new VertexPropertyCursor(
                        vertexInfo,
                        datasetRoot,
                        physicalReader,
                        runs(selected),
                        properties,
                        Long.MAX_VALUE)) {
            while (cursor.next()) {
                GraphVertex vertex = cursor.vertex();
                result.put(vertex.id(), vertex.properties());
            }
        }
        if (!result.keySet().equals(selected)) {
            throw new IllegalArgumentException(
                    "GraphAr vertex chunks did not return every selected vertex.");
        }
        return result;
    }

    /** Opens a cursor over every vertex of this type with all declared properties. */
    public VertexPropertyCursor scan() throws IOException {
        return scan(VertexPropertyCursor.allPropertyNames(vertexInfo), Long.MAX_VALUE);
    }

    /** Opens a cursor over every vertex with only the requested properties. */
    public VertexPropertyCursor scan(Collection<String> properties) throws IOException {
        return scan(properties, Long.MAX_VALUE);
    }

    /**
     * Opens a bounded cursor over vertices in ascending index order. The limit is applied before
     * opening chunks that cannot contribute rows.
     */
    public VertexPropertyCursor scan(Collection<String> properties, long limit) throws IOException {
        if (limit < 0) {
            throw new IllegalArgumentException("Vertex limit cannot be negative.");
        }
        long vertexCount = vertexCount();
        long chunkSize = vertexInfo.getChunkSize();
        long chunkCount = ChunkMath.chunkCount(vertexCount, chunkSize);
        List<VertexPropertyCursor.Segment> segments = new ArrayList<>();
        long remaining = limit;
        for (long chunk = 0; chunk < chunkCount && remaining > 0; chunk++) {
            long rows = Math.min(chunkSize, vertexCount - Math.multiplyExact(chunk, chunkSize));
            rows = Math.min(rows, remaining);
            segments.add(new VertexPropertyCursor.Segment(chunk, new RowRange(0, rows)));
            remaining -= rows;
        }
        return new VertexPropertyCursor(
                vertexInfo, datasetRoot, physicalReader, segments, properties, limit);
    }

    private List<VertexPropertyCursor.Segment> runs(TreeSet<Long> selected) {
        long chunkSize = vertexInfo.getChunkSize();
        List<VertexPropertyCursor.Segment> segments = new ArrayList<>();
        long runStart = -1;
        long previous = -1;
        for (long vertexId : selected) {
            boolean continues =
                    runStart >= 0
                            && vertexId == previous + 1
                            && ChunkMath.chunkIndex(vertexId, chunkSize)
                                    == ChunkMath.chunkIndex(runStart, chunkSize);
            if (!continues) {
                if (runStart >= 0) {
                    segments.add(segment(runStart, previous, chunkSize));
                }
                runStart = vertexId;
            }
            previous = vertexId;
        }
        segments.add(segment(runStart, previous, chunkSize));
        return segments;
    }

    private static VertexPropertyCursor.Segment segment(
            long runStart, long runEnd, long chunkSize) {
        long chunk = ChunkMath.chunkIndex(runStart, chunkSize);
        long start = ChunkMath.offsetInChunk(runStart, chunkSize);
        return new VertexPropertyCursor.Segment(
                chunk, new RowRange(start, start + (runEnd - runStart) + 1));
    }

    private static void requireVertexId(long vertexId, long vertexCount) {
        if (vertexId < 0 || vertexId >= vertexCount) {
            throw new IllegalArgumentException(
                    "Vertex ID is outside this vertex type: " + vertexId + '.');
        }
    }
}
