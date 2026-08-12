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
import java.util.Objects;
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
 * position. This preserves distinct properties for parallel topology rows.
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
    private final List<ReadReport> reports = new ArrayList<>();
    private int segmentIndex;
    private List<GraphEdge> rows = List.of();
    private int rowIndex;
    private GraphEdge current;
    private boolean closed;

    EdgePropertyCursor(
            EdgeInfo edgeInfo,
            AdjListType layout,
            URI datasetRoot,
            PhysicalReader physicalReader,
            List<Segment> segments,
            Long selectedAlignedVertex) {
        this.edgeInfo = Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        this.layout = Objects.requireNonNull(layout, "Adjacency layout cannot be null.");
        this.datasetRoot = DatasetUris.directory(datasetRoot);
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
        this.segments = List.copyOf(segments);
        this.selectedAlignedVertex = selectedAlignedVertex;
    }

    /** Advances to the next selected topology row. */
    public boolean next() throws IOException {
        current = null;
        while (!closed) {
            if (rowIndex < rows.size()) {
                GraphEdge candidate = rows.get(rowIndex++);
                if (selectedAlignedVertex == null || aligned(candidate) == selectedAlignedVertex) {
                    current = candidate;
                    return true;
                }
                continue;
            }
            if (segmentIndex == segments.size()) {
                close();
                return false;
            }
            rows = readSegment(segments.get(segmentIndex++));
            rowIndex = 0;
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
    public void close() {
        closed = true;
        rows = List.of();
        current = null;
    }

    private List<GraphEdge> readSegment(Segment segment) throws IOException {
        List<Object[]> topology =
                readRows(
                        DatasetUris.resolve(
                                datasetRoot,
                                edgeInfo.getAdjacentListChunkUri(
                                        layout, segment.partition, segment.edgeChunk)),
                        TOPOLOGY_COLUMNS,
                        segment.range);
        if (topology.size() != segment.range.endExclusive() - segment.range.startInclusive()) {
            throw new IllegalArgumentException(
                    "Topology chunk row count does not match its GraphAr edge count.");
        }
        List<List<Object[]>> propertyRows = new ArrayList<>();
        List<PropertyGroup> groups = propertyGroups();
        for (PropertyGroup group : groups) {
            List<String> names = new ArrayList<>();
            for (Property property : group) names.add(property.getName());
            List<Object[]> values =
                    readRows(
                            DatasetUris.resolve(
                                    datasetRoot,
                                    edgeInfo.getPropertyGroupChunkUri(
                                            group, layout, segment.partition, segment.edgeChunk)),
                            names,
                            segment.range);
            if (values.size() != topology.size()) {
                throw new IllegalArgumentException(
                        "Edge property chunk row count does not match its topology chunk.");
            }
            propertyRows.add(values);
        }
        List<GraphEdge> result = new ArrayList<>(topology.size());
        for (int row = 0; row < topology.size(); row++) {
            Object[] topologyRow = topology.get(row);
            long source = id(topologyRow[0], "source");
            long destination = id(topologyRow[1], "destination");
            Map<String, Object> properties = new LinkedHashMap<>();
            for (int groupIndex = 0; groupIndex < groups.size(); groupIndex++) {
                int propertyIndex = 0;
                for (Property property : groups.get(groupIndex)) {
                    properties.put(
                            property.getName(),
                            propertyRows.get(groupIndex).get(row)[propertyIndex++]);
                }
            }
            result.add(new GraphEdge(source, destination, properties));
        }
        return result;
    }

    private List<Object[]> readRows(URI uri, List<String> projection, RowRange range)
            throws IOException {
        ReadResult result =
                physicalReader.read(
                        ReadRequest.builder(uri)
                                .projection(Projection.of(projection))
                                .rowRange(range)
                                .build());
        reports.add(result.report());
        List<Object[]> values = new ArrayList<>();
        try (BatchCursor cursor = result.cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                for (int row = 0; row < batch.rowCount(); row++) {
                    Object[] value = new Object[projection.size()];
                    for (int column = 0; column < value.length; column++) {
                        value[column] = batch.row(row).value(column);
                    }
                    values.add(value);
                }
            }
        }
        return values;
    }

    private List<PropertyGroup> propertyGroups() {
        List<PropertyGroup> groups = new ArrayList<>();
        for (int index = 0; index < edgeInfo.getPropertyGroupNum(); index++) {
            groups.add(edgeInfo.getPropertyGroupByIndex(index));
        }
        return groups;
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
}
