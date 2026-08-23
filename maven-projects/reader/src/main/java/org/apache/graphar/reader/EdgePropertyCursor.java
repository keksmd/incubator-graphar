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
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RowRange;

/**
 * A closeable cursor that joins GraphAr topology and edge property chunks by their physical row
 * position. Topology and selected property groups are consumed in lockstep, so the cursor never
 * materializes a complete edge chunk before returning its first row.
 */
public final class EdgePropertyCursor implements AutoCloseable {
    private static final List<String> TOPOLOGY_COLUMNS =
            List.of("_graphArSrcIndex", "_graphArDstIndex");

    private final EdgeInfo edgeInfo;
    private final AdjListType layout;
    private final URI datasetRoot;
    private final PhysicalReader physicalReader;
    private final List<Segment> segments;
    private final Long selectedAlignedVertex;
    private final List<PropertyProjection> propertyProjections;
    private final long limit;
    private final List<ReadReport> reports = new ArrayList<>();
    private int segmentIndex;
    private long emitted;
    private SegmentStreams streams;
    private GraphEdge current;
    private boolean closed;

    EdgePropertyCursor(
            EdgeInfo edgeInfo,
            AdjListType layout,
            URI datasetRoot,
            PhysicalReader physicalReader,
            List<Segment> segments,
            Long selectedAlignedVertex,
            Collection<String> properties,
            long limit) {
        this.edgeInfo = Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        this.layout = Objects.requireNonNull(layout, "Adjacency layout cannot be null.");
        this.datasetRoot = DatasetUris.directory(datasetRoot);
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
        this.segments = List.copyOf(segments);
        this.selectedAlignedVertex = selectedAlignedVertex;
        this.propertyProjections = propertyProjections(edgeInfo, properties);
        if (limit < 0) {
            throw new IllegalArgumentException("Edge limit cannot be negative.");
        }
        this.limit = limit;
    }

    /** Advances to the next selected topology row. */
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
            if (!streams.topology.next()) {
                SegmentStreams completed = streams;
                streams = null;
                finish(completed);
                continue;
            }
            Map<String, Object> properties = new LinkedHashMap<>();
            for (int index = 0; index < streams.properties.size(); index++) {
                PropertyStream property = streams.properties.get(index);
                if (!property.rows.next()) {
                    throw new IllegalArgumentException(
                            "Edge property chunk row count does not match its topology chunk.");
                }
                List<String> names = property.projection.names;
                for (int column = 0; column < names.size(); column++) {
                    properties.put(names.get(column), property.rows.value(column));
                }
            }
            GraphEdge candidate =
                    new GraphEdge(
                            id(streams.topology.value(0), "source"),
                            id(streams.topology.value(1), "destination"),
                            properties);
            if (selectedAlignedVertex == null || aligned(candidate) == selectedAlignedVertex) {
                current = candidate;
                emitted++;
                return true;
            }
        }
        return false;
    }

    /** Returns the current topology row after {@link #next()} returns {@code true}. */
    public GraphEdge edge() {
        if (current == null) {
            throw new IllegalStateException("No current edge. Call next() first.");
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

    private SegmentStreams openSegment(Segment segment) throws IOException {
        List<BatchStream> opened = new ArrayList<>();
        try {
            BatchStream topology =
                    open(
                            DatasetUris.resolve(
                                    datasetRoot,
                                    edgeInfo.getAdjacentListChunkUri(
                                            layout, segment.partition, segment.edgeChunk)),
                            TOPOLOGY_COLUMNS,
                            segment.range,
                            opened);
            List<PropertyStream> properties = new ArrayList<>();
            for (PropertyProjection projection : propertyProjections) {
                properties.add(
                        new PropertyStream(
                                projection,
                                open(
                                        DatasetUris.resolve(
                                                datasetRoot,
                                                edgeInfo.getPropertyGroupChunkUri(
                                                        projection.group,
                                                        layout,
                                                        segment.partition,
                                                        segment.edgeChunk)),
                                        projection.names,
                                        segment.range,
                                        opened)));
            }
            return new SegmentStreams(topology, properties);
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

    private BatchStream open(
            URI uri, List<String> projection, RowRange range, List<BatchStream> opened)
            throws IOException {
        ReadResult result =
                physicalReader.read(
                        ReadRequest.builder(uri)
                                .projection(Projection.of(projection))
                                .rowRange(range)
                                .build());
        reports.add(result.report());
        BatchStream rows = new BatchStream(result.cursor());
        opened.add(rows);
        return rows;
    }

    private static IOException closeAll(List<BatchStream> streams) {
        IOException failure = null;
        for (BatchStream stream : streams) {
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

    private static List<PropertyProjection> propertyProjections(
            EdgeInfo edgeInfo, Collection<String> selectedProperties) {
        Objects.requireNonNull(selectedProperties, "Selected edge properties cannot be null.");
        Set<String> requested = new LinkedHashSet<>();
        for (String property : selectedProperties) {
            if (property == null || property.isBlank()) {
                throw new IllegalArgumentException("Selected edge property names cannot be blank.");
            }
            if (!requested.add(property)) {
                throw new IllegalArgumentException(
                        "Selected edge property is duplicated: " + property);
            }
        }
        List<PropertyProjection> projections = new ArrayList<>();
        Set<String> found = new LinkedHashSet<>();
        for (int index = 0; index < edgeInfo.getPropertyGroupNum(); index++) {
            PropertyGroup group = edgeInfo.getPropertyGroupByIndex(index);
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
            throw new IllegalArgumentException("Edge info does not declare properties: " + unknown);
        }
        return List.copyOf(projections);
    }

    static List<String> allPropertyNames(EdgeInfo edgeInfo) {
        List<String> names = new ArrayList<>();
        for (int index = 0; index < edgeInfo.getPropertyGroupNum(); index++) {
            for (Property property : edgeInfo.getPropertyGroupByIndex(index)) {
                names.add(property.getName());
            }
        }
        return List.copyOf(names);
    }

    private long aligned(GraphEdge edge) {
        return layout.getAlignedBy().equals("src") ? edge.source() : edge.destination();
    }

    private static long id(Object value, String kind) {
        if (!(value instanceof Long) || (Long) value < 0) {
            throw new IllegalArgumentException(
                    "GraphAr " + kind + " IDs must be non-negative INT64 values.");
        }
        return (Long) value;
    }

    static final class Segment {
        private final long partition;
        private final long edgeChunk;
        private final RowRange range;

        Segment(long partition, long edgeChunk, RowRange range) {
            this.partition = partition;
            this.edgeChunk = edgeChunk;
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
        private final BatchStream rows;

        private PropertyStream(PropertyProjection projection, BatchStream rows) {
            this.projection = projection;
            this.rows = rows;
        }
    }

    private static final class SegmentStreams implements AutoCloseable {
        private final BatchStream topology;
        private final List<PropertyStream> properties;

        private SegmentStreams(BatchStream topology, List<PropertyStream> properties) {
            this.topology = topology;
            this.properties = List.copyOf(properties);
        }

        private void verifyExhausted() throws IOException {
            for (PropertyStream property : properties) {
                if (property.rows.next()) {
                    throw new IllegalArgumentException(
                            "Edge property chunk row count does not match its topology chunk.");
                }
            }
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            try {
                topology.close();
            } catch (IOException exception) {
                failure = exception;
            }
            for (PropertyStream property : properties) {
                try {
                    property.rows.close();
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

    private static final class BatchStream implements AutoCloseable {
        private final BatchCursor cursor;
        private RecordBatch batch;
        private int row;
        private boolean exhausted;

        private BatchStream(BatchCursor cursor) {
            this.cursor = Objects.requireNonNull(cursor, "Batch cursor cannot be null.");
        }

        private boolean next() throws IOException {
            while (!exhausted) {
                if (batch != null && row < batch.rowCount()) {
                    row++;
                    return true;
                }
                if (!cursor.next()) {
                    exhausted = true;
                    batch = null;
                    return false;
                }
                batch = Objects.requireNonNull(cursor.batch(), "Batch cursor returned null batch.");
                row = 0;
            }
            return false;
        }

        private Object value(int column) {
            if (batch == null || row == 0) {
                throw new IllegalStateException("No current batch row. Call next() first.");
            }
            return batch.column(column).getObject(row - 1);
        }

        @Override
        public void close() throws IOException {
            cursor.close();
        }
    }
}
