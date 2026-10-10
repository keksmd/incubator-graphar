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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.RowRange;

/**
 * A closeable cursor over vertex chunks that joins the selected property groups by row position. A
 * vertex's internal ID is its chunk index times the chunk size plus its row in the chunk.
 */
public final class VertexCursor implements AutoCloseable {
    private final VertexInfo vertexInfo;
    private final URI datasetRoot;
    private final PhysicalReader physicalReader;
    private final List<Segment> segments;
    private final List<PropertySelection> selections;
    private final List<ReadReport> reports = new ArrayList<>();
    private int segmentIndex;
    private AlignedRows rows;
    private long nextId;
    private GraphVertex current;
    private boolean closed;

    VertexCursor(
            VertexInfo vertexInfo,
            URI datasetRoot,
            PhysicalReader physicalReader,
            List<Segment> segments,
            List<PropertySelection> selections) {
        this.vertexInfo = vertexInfo;
        this.datasetRoot = datasetRoot;
        this.physicalReader = physicalReader;
        this.segments = List.copyOf(segments);
        this.selections = selections;
    }

    /**
     * Advances to the next vertex.
     *
     * @return whether a vertex is available
     * @throws IOException if a chunk cannot be read
     */
    public boolean next() throws IOException {
        current = null;
        while (!closed) {
            if (rows == null) {
                if (segmentIndex == segments.size()) {
                    close();
                    return false;
                }
                Segment segment = segments.get(segmentIndex++);
                rows = open(segment);
                nextId = segment.firstId;
                continue;
            }
            if (!rows.next()) {
                AlignedRows finished = rows;
                rows = null;
                finished.close();
                continue;
            }
            Map<String, Object> properties = new LinkedHashMap<>();
            for (int chunk = 0; chunk < selections.size(); chunk++) {
                List<String> names = selections.get(chunk).names;
                for (int column = 0; column < names.size(); column++) {
                    properties.put(names.get(column), rows.value(chunk, column));
                }
            }
            current = new GraphVertex(nextId++, properties);
            return true;
        }
        return false;
    }

    /**
     * Returns the current vertex after {@link #next()} returns {@code true}.
     *
     * @return the current vertex
     */
    public GraphVertex vertex() {
        if (current == null) {
            throw new IllegalStateException("No current vertex. Call next() first.");
        }
        return current;
    }

    /**
     * Returns the physical read reports of the chunks opened so far, in request order.
     *
     * @return the read reports
     */
    public List<ReadReport> reports() {
        return List.copyOf(reports);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        current = null;
        if (rows != null) {
            AlignedRows open = rows;
            rows = null;
            open.close();
        }
    }

    private AlignedRows open(Segment segment) throws IOException {
        List<URI> uris = new ArrayList<>();
        List<List<String>> columns = new ArrayList<>();
        for (PropertySelection selection : selections) {
            uris.add(
                    DatasetUris.resolve(
                            datasetRoot,
                            vertexInfo.getPropertyGroupChunkUri(selection.group, segment.chunk)));
            columns.add(selection.names);
        }
        return AlignedRows.open(
                physicalReader, uris, columns, new RowRange(0, segment.rows), reports);
    }

    static final class Segment {
        private final long chunk;
        private final long firstId;
        private final long rows;

        Segment(long chunk, long firstId, long rows) {
            this.chunk = chunk;
            this.firstId = firstId;
            this.rows = rows;
        }
    }
}
