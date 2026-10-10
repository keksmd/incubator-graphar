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
import java.util.List;
import java.util.Objects;
import org.apache.graphar.core.ChunkMath;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.storage.Storage;

/**
 * Reads the property chunks of one GraphAr vertex type. A scan opens only the property groups that
 * hold a requested property and projects each to those columns.
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

    /**
     * Returns the metadata that defines this vertex type.
     *
     * @return the vertex metadata
     */
    public VertexInfo vertexInfo() {
        return vertexInfo;
    }

    /**
     * Returns the number of vertices recorded in the vertex count file.
     *
     * @return the vertex count
     * @throws IOException if the count file cannot be read
     */
    public long vertexCount() throws IOException {
        return ControlFileReader.readNonNegativeLong(
                storage, DatasetUris.resolve(datasetRoot, vertexInfo.getVerticesNumFileUri()));
    }

    /**
     * Opens a cursor over every vertex with every declared property.
     *
     * @return the vertex cursor
     * @throws IOException if the vertex count cannot be read
     */
    public VertexCursor scan() throws IOException {
        return scan(PropertySelection.allNames(vertexInfo.getPropertyGroups()));
    }

    /**
     * Opens a cursor over every vertex, chunk by chunk, with only the requested properties.
     *
     * @param properties the property names to read; at least one
     * @return the vertex cursor
     * @throws IOException if the vertex count cannot be read
     */
    public VertexCursor scan(Collection<String> properties) throws IOException {
        long vertexCount = vertexCount();
        List<VertexCursor.Segment> segments = new ArrayList<>();
        for (long chunk = 0;
                chunk < ChunkMath.chunkCount(vertexCount, vertexInfo.getChunkSize());
                chunk++) {
            segments.add(segment(chunk, vertexCount));
        }
        return open(segments, properties);
    }

    /**
     * Opens a cursor over the vertices of one chunk with only the requested properties.
     *
     * @param chunkIndex the vertex chunk
     * @param properties the property names to read; at least one
     * @return the vertex cursor
     * @throws IOException if the vertex count cannot be read
     */
    public VertexCursor scanChunk(long chunkIndex, Collection<String> properties)
            throws IOException {
        long vertexCount = vertexCount();
        if (chunkIndex < 0
                || chunkIndex >= ChunkMath.chunkCount(vertexCount, vertexInfo.getChunkSize())) {
            throw new IllegalArgumentException(
                    "Vertex chunk is outside this vertex type: " + chunkIndex);
        }
        return open(List.of(segment(chunkIndex, vertexCount)), properties);
    }

    private VertexCursor.Segment segment(long chunk, long vertexCount) {
        long first = Math.multiplyExact(chunk, vertexInfo.getChunkSize());
        return new VertexCursor.Segment(
                chunk, first, Math.min(vertexInfo.getChunkSize(), vertexCount - first));
    }

    private VertexCursor open(List<VertexCursor.Segment> segments, Collection<String> properties) {
        List<PropertySelection> selections =
                PropertySelection.select(vertexInfo.getPropertyGroups(), properties);
        if (selections.isEmpty()) {
            throw new IllegalArgumentException("A vertex scan must select at least one property.");
        }
        return new VertexCursor(vertexInfo, datasetRoot, physicalReader, segments, selections);
    }
}
