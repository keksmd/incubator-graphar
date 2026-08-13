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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Row;
import org.apache.graphar.io.RowRange;

/**
 * A closeable cursor that joins the selected GraphAr vertex property groups of one vertex type by
 * their physical row position. Only the requested chunks and columns are opened, so a projected
 * read of a few vertices never materializes a whole vertex chunk.
 */
public final class VertexPropertyCursor implements AutoCloseable {
    static final String VERTEX_INDEX_COLUMN = "_graphArVertexIndex";

    private final VertexInfo vertexInfo;
    private final URI datasetRoot;
    private final PhysicalReader physicalReader;
    private final List<Segment> segments;
    private final List<PropertyProjection> propertyProjections;
    private final long limit;
    private final List<ReadReport> reports = new ArrayList<>();
    private int segmentIndex;
    private long emitted;
    private SegmentStreams streams;
    private GraphVertex current;
    private boolean closed;

    VertexPropertyCursor(
            VertexInfo vertexInfo,
            URI datasetRoot,
            PhysicalReader physicalReader,
            List<Segment> segments,
            Collection<String> properties,
            long limit) {
        this.vertexInfo = Objects.requireNonNull(vertexInfo, "Vertex info cannot be null.");
        this.datasetRoot = DatasetUris.directory(datasetRoot);
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
        this.segments = List.copyOf(segments);
        this.propertyProjections = propertyProjections(vertexInfo, properties);
        if (limit < 0) {
            throw new IllegalArgumentException("Vertex limit cannot be negative.");
        }
        this.limit = limit;
    }

    /** Advances to the next selected vertex row. */
    public boolean next() throws IOException {
        current = null;
        while (!closed) {
            if (emitted == limit) {
                close();
                return false;
            }
            if (streams == null) {
                if (segmentIndex == segments.size()) {
                    close();
                    return false;
                }
                streams = openSegment(segments.get(segmentIndex++));
            }
            Row leading = streams.groups.get(0).rows.next();
            if (leading == null) {
                SegmentStreams completed = streams;
                streams = null;
                finish(completed);
                continue;
            }
            long id = vertexId(leading.value(0));
            Map<String, Object> properties = new LinkedHashMap<>();
            copyProperties(streams.groups.get(0).projection, leading, properties);
            for (int index = 1; index < streams.groups.size(); index++) {
                PropertyStream stream = streams.groups.get(index);
                Row row = stream.rows.next();
                if (row == null) {
                    throw new IllegalArgumentException(
                            "Vertex property chunk row count does not match its leading chunk.");
                }
                if (vertexId(row.value(0)) != id) {
                    throw new IllegalArgumentException(
                            "Vertex property groups disagree on the vertex index at row "
                                    + emitted
                                    + '.');
                }
                copyProperties(stream.projection, row, properties);
            }
            current = new GraphVertex(id, properties);
            emitted++;
            return true;
        }
        return false;
    }

    /** Returns the current vertex after {@link #next()} returns {@code true}. */
    public GraphVertex vertex() {
        if (current == null) {
            throw new IllegalStateException("No current vertex. Call next() first.");
        }
        return current;
    }

    /** Returns immutable physical read reports in request order. */
    public List<ReadReport> reports() {
        return List.copyOf(reports);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        current = null;
        if (streams != null) {
            streams.close();
            streams = null;
        }
    }

    private static void copyProperties(
            PropertyProjection projection, Row row, Map<String, Object> target) {
        for (int column = 0; column < projection.names.size(); column++) {
            target.put(projection.names.get(column), row.value(column + 1));
        }
    }

    private SegmentStreams openSegment(Segment segment) throws IOException {
        List<RowStream> opened = new ArrayList<>();
        try {
            List<PropertyStream> groups = new ArrayList<>();
            for (PropertyProjection projection : propertyProjections) {
                List<String> columns = new ArrayList<>();
                columns.add(VERTEX_INDEX_COLUMN);
                columns.addAll(projection.names);
                groups.add(
                        new PropertyStream(
                                projection,
                                open(
                                        DatasetUris.resolve(
                                                datasetRoot,
                                                vertexInfo.getPropertyGroupChunkUri(
                                                        projection.group, segment.chunk)),
                                        columns,
                                        segment.range,
                                        opened)));
            }
            return new SegmentStreams(groups);
        } catch (IOException | RuntimeException exception) {
            IOException closeFailure = closeAll(opened);
            if (closeFailure != null) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }

    private static void finish(SegmentStreams streams) throws IOException {
        try {
            streams.verifyExhausted();
        } catch (IOException | RuntimeException exception) {
            try {
                streams.close();
            } catch (IOException closeException) {
                exception.addSuppressed(closeException);
            }
            throw exception;
        }
        streams.close();
    }

    private RowStream open(URI uri, List<String> projection, RowRange range, List<RowStream> opened)
            throws IOException {
        ReadResult result =
                physicalReader.read(
                        ReadRequest.builder(uri)
                                .projection(Projection.of(projection))
                                .rowRange(range)
                                .build());
        reports.add(result.report());
        RowStream rows = new RowStream(result.cursor());
        opened.add(rows);
        return rows;
    }

    private static IOException closeAll(List<RowStream> streams) {
        IOException failure = null;
        for (RowStream stream : streams) {
            try {
                stream.close();
            } catch (IOException exception) {
                if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }
        return failure;
    }

    private static long vertexId(Object value) {
        if (!(value instanceof Long) || (Long) value < 0) {
            throw new IllegalArgumentException(
                    "GraphAr vertex indices must be non-negative INT64 values.");
        }
        return (Long) value;
    }

    private static List<PropertyProjection> propertyProjections(
            VertexInfo vertexInfo, Collection<String> selectedProperties) {
        Objects.requireNonNull(selectedProperties, "Selected vertex properties cannot be null.");
        Set<String> requested = new LinkedHashSet<>();
        for (String property : selectedProperties) {
            if (property == null || property.isBlank()) {
                throw new IllegalArgumentException(
                        "Selected vertex property names cannot be blank.");
            }
            if (!requested.add(property)) {
                throw new IllegalArgumentException(
                        "Selected vertex property is duplicated: " + property);
            }
        }
        List<PropertyProjection> projections = new ArrayList<>();
        Set<String> found = new LinkedHashSet<>();
        for (int index = 0; index < vertexInfo.getPropertyGroupNum(); index++) {
            PropertyGroup group = vertexInfo.getPropertyGroupByIndex(index);
            List<String> names = new ArrayList<>();
            for (Property property : group) {
                if (requested.contains(property.getName())) {
                    names.add(property.getName());
                    found.add(property.getName());
                }
            }
            if (!names.isEmpty()) {
                projections.add(new PropertyProjection(group, names));
            }
        }
        if (!found.equals(requested)) {
            Set<String> unknown = new LinkedHashSet<>(requested);
            unknown.removeAll(found);
            throw new IllegalArgumentException(
                    "Vertex info does not declare properties: " + unknown);
        }
        if (projections.isEmpty()) {
            projections.add(
                    new PropertyProjection(vertexInfo.getPropertyGroupByIndex(0), List.of()));
        }
        return List.copyOf(projections);
    }

    static List<String> allPropertyNames(VertexInfo vertexInfo) {
        List<String> names = new ArrayList<>();
        for (int index = 0; index < vertexInfo.getPropertyGroupNum(); index++) {
            for (Property property : vertexInfo.getPropertyGroupByIndex(index)) {
                names.add(property.getName());
            }
        }
        return List.copyOf(names);
    }

    static final class Segment {
        private final long chunk;
        private final RowRange range;

        Segment(long chunk, RowRange range) {
            this.chunk = chunk;
            this.range = range;
        }
    }

    private static final class PropertyProjection {
        private final PropertyGroup group;
        private final List<String> names;

        private PropertyProjection(PropertyGroup group, List<String> names) {
            this.group = group;
            this.names = List.copyOf(names);
        }
    }

    private static final class PropertyStream {
        private final PropertyProjection projection;
        private final RowStream rows;

        private PropertyStream(PropertyProjection projection, RowStream rows) {
            this.projection = projection;
            this.rows = rows;
        }
    }

    private static final class SegmentStreams implements AutoCloseable {
        private final List<PropertyStream> groups;

        private SegmentStreams(List<PropertyStream> groups) {
            this.groups = List.copyOf(groups);
        }

        private void verifyExhausted() throws IOException {
            for (int index = 1; index < groups.size(); index++) {
                if (groups.get(index).rows.next() != null) {
                    throw new IllegalArgumentException(
                            "Vertex property chunk row count does not match its leading chunk.");
                }
            }
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            for (PropertyStream group : groups) {
                try {
                    group.rows.close();
                } catch (IOException exception) {
                    if (failure == null) {
                        failure = exception;
                    } else {
                        failure.addSuppressed(exception);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class RowStream implements AutoCloseable {
        private final BatchCursor cursor;
        private RecordBatch batch;
        private int row;
        private boolean exhausted;

        private RowStream(BatchCursor cursor) {
            this.cursor = Objects.requireNonNull(cursor, "Batch cursor cannot be null.");
        }

        private Row next() throws IOException {
            while (!exhausted) {
                if (batch != null && row < batch.rowCount()) {
                    return batch.row(row++);
                }
                if (!cursor.next()) {
                    exhausted = true;
                    batch = null;
                    return null;
                }
                batch = Objects.requireNonNull(cursor.batch(), "batch cursor returned null");
                row = 0;
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            batch = null;
            exhausted = true;
            cursor.close();
        }
    }
}
