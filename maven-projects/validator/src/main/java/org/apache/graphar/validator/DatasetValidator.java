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

package org.apache.graphar.validator;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.Cardinality;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Row;
import org.apache.graphar.io.Schema;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.SeekableInput;
import org.apache.graphar.storage.Storage;

/**
 * Streams every metadata-addressable GraphAr data file and verifies its control, schema, topology
 * and row-alignment invariants. The validator intentionally depends only on neutral IO contracts.
 */
public final class DatasetValidator {
    private static final Field VERTEX_INDEX_FIELD =
            new Field("_graphArVertexIndex", ColumnType.of(ColumnType.Kind.INT64), false);
    private static final Schema OFFSET_SCHEMA =
            new Schema(
                    List.of(
                            new Field(
                                    "_graphArOffset",
                                    ColumnType.of(ColumnType.Kind.INT64),
                                    false)));
    private static final Schema TOPOLOGY_SCHEMA =
            new Schema(
                    List.of(
                            new Field(
                                    "_graphArSrcIndex",
                                    ColumnType.of(ColumnType.Kind.INT64),
                                    false),
                            new Field(
                                    "_graphArDstIndex",
                                    ColumnType.of(ColumnType.Kind.INT64),
                                    false)));

    /**
     * Validates a dataset rooted at {@code datasetRoot}; all corruption is reported, not hidden.
     */
    public ValidationReport validate(
            GraphInfo graphInfo, URI datasetRoot, Storage storage, PhysicalReader physicalReader) {
        Objects.requireNonNull(graphInfo, "Graph info cannot be null.");
        Objects.requireNonNull(datasetRoot, "Dataset root cannot be null.");
        Objects.requireNonNull(storage, "Storage cannot be null.");
        Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
        ValidationReport.Collector report = new ValidationReport.Collector();
        URI root = directory(datasetRoot);
        Map<String, Long> vertexCounts = new HashMap<>();
        for (VertexInfo vertex : graphInfo.getVertexInfos()) {
            Long count = validateVertex(vertex, root, storage, physicalReader, report);
            if (count != null) {
                vertexCounts.put(vertex.getType(), count);
            }
        }
        for (EdgeInfo edge : graphInfo.getEdgeInfos()) {
            validateEdge(edge, root, storage, physicalReader, vertexCounts, report);
        }
        return report.build();
    }

    /** Validates a dataset and throws with the complete report when any invariant is violated. */
    public void validateOrThrow(
            GraphInfo graphInfo, URI datasetRoot, Storage storage, PhysicalReader physicalReader) {
        ValidationReport report = validate(graphInfo, datasetRoot, storage, physicalReader);
        if (!report.isValid()) {
            throw new IllegalStateException("Invalid GraphAr dataset: " + report.issues());
        }
    }

    private Long validateVertex(
            VertexInfo vertex,
            URI root,
            Storage storage,
            PhysicalReader reader,
            ValidationReport.Collector report) {
        Long count = null;
        for (PropertyGroup group : vertex.getPropertyGroups()) {
            URI control = absolute(root, vertex.getVerticesNumFileUri());
            Long groupCount = readControl(storage, control, report);
            if (groupCount == null) {
                continue;
            }
            if (count != null && count.longValue() != groupCount.longValue()) {
                report.error(
                        "VERTEX_COUNT_MISMATCH",
                        control,
                        "Vertex property groups disagree on vertex_count.");
            }
            count = groupCount;
            validateChunks(
                    root,
                    groupCount,
                    vertex.getChunkSize(),
                    index -> vertex.getPropertyGroupChunkUri(group, index),
                    vertexSchema(group),
                    storage,
                    reader,
                    report,
                    new VertexIndexCheck(vertex.getChunkSize(), report));
        }
        if (count == null) {
            report.warning(
                    "VERTEX_COUNT_UNAVAILABLE",
                    absolute(root, vertex.getBaseUri()),
                    "No vertex property group supplied a vertex_count control file.");
        }
        return count;
    }

    private void validateEdge(
            EdgeInfo edge,
            URI root,
            Storage storage,
            PhysicalReader reader,
            Map<String, Long> vertexCounts,
            ValidationReport.Collector report) {
        for (AdjListType layout : edge.getAdjacentLists().keySet()) {
            Long alignedCount =
                    vertexCounts.get(
                            layout.getAlignedBy().equals("src")
                                    ? edge.getSrcType()
                                    : edge.getDstType());
            Long sourceCount = vertexCounts.get(edge.getSrcType());
            Long destinationCount = vertexCounts.get(edge.getDstType());
            URI vertexControl = absolute(root, edge.getVerticesNumFileUri(layout));
            Long controlAlignedCount = readControl(storage, vertexControl, report);
            if (alignedCount != null
                    && controlAlignedCount != null
                    && alignedCount.longValue() != controlAlignedCount.longValue()) {
                report.error(
                        "EDGE_VERTEX_COUNT_MISMATCH",
                        vertexControl,
                        "Layout vertex_count disagrees with its aligned vertex type.");
            }
            long partitions =
                    count(
                            controlAlignedCount != null ? controlAlignedCount : alignedCount,
                            chunkSize(edge, layout));
            for (long part = 0; part < partitions; part++) {
                URI edgeControl = absolute(root, edge.getEdgesNumFileUri(layout, part));
                Long edgeCount = readControl(storage, edgeControl, report);
                if (edgeCount == null) {
                    continue;
                }
                long currentPart = part;
                TopologyCheck topology =
                        new TopologyCheck(
                                layout,
                                currentPart,
                                chunkSize(edge, layout),
                                sourceCount,
                                destinationCount,
                                report);
                validateChunks(
                        root,
                        edgeCount,
                        edge.getChunkSize(),
                        index -> edge.getAdjacentListChunkUri(layout, currentPart, index),
                        TOPOLOGY_SCHEMA,
                        storage,
                        reader,
                        report,
                        topology);
                if (topology.rows != edgeCount) {
                    report.error(
                            "TOPOLOGY_ROW_COUNT",
                            edgeControl,
                            "Topology rows do not equal edge_count.");
                }
                for (PropertyGroup group : edge.getPropertyGroups()) {
                    validateChunks(
                            root,
                            edgeCount,
                            edge.getChunkSize(),
                            index ->
                                    edge.getPropertyGroupChunkUri(
                                            group, layout, currentPart, index),
                            propertySchema(group),
                            storage,
                            reader,
                            report,
                            null);
                }
                validateOffsets(
                        edge,
                        layout,
                        currentPart,
                        edgeCount,
                        controlAlignedCount,
                        root,
                        storage,
                        reader,
                        report);
            }
        }
    }

    private void validateOffsets(
            EdgeInfo edge,
            AdjListType layout,
            long part,
            long edgeCount,
            Long alignedCount,
            URI root,
            Storage storage,
            PhysicalReader reader,
            ValidationReport.Collector report) {
        URI uri = absolute(root, edge.getOffsetChunkUri(layout, part));
        if (!layout.isOrdered()) {
            if (present(storage, uri, report)) {
                report.error(
                        "UNORDERED_OFFSET",
                        uri,
                        "Unordered layouts must not publish offset files.");
            }
            return;
        }
        if (alignedCount == null) {
            report.error(
                    "OFFSET_ALIGNMENT_UNKNOWN",
                    uri,
                    "Cannot validate offsets without aligned vertex_count.");
            return;
        }
        long firstVertex = part * chunkSize(edge, layout);
        long vertices = Math.min(chunkSize(edge, layout), alignedCount - firstVertex);
        OffsetCheck offsets = new OffsetCheck(vertices + 1, edgeCount, report);
        long rows = scan(uri, OFFSET_SCHEMA, reader, report, offsets);
        if (rows != vertices + 1) {
            report.error(
                    "OFFSET_ROW_COUNT",
                    uri,
                    "Offset rows must equal the aligned vertex count plus one.");
        }
        offsets.finish(uri);
    }

    private void validateChunks(
            URI root,
            long totalRows,
            long chunkSize,
            UriFactory factory,
            Schema expected,
            Storage storage,
            PhysicalReader reader,
            ValidationReport.Collector report,
            RowCheck rowCheck) {
        for (long chunk = 0; chunk < count(totalRows, chunkSize); chunk++) {
            long expectedRows = Math.min(chunkSize, totalRows - chunk * chunkSize);
            URI uri = absolute(root, factory.uri(chunk));
            if (!exists(storage, uri, report, ignored -> {})) continue;
            long actualRows = scan(uri, expected, reader, report, rowCheck);
            if (actualRows != expectedRows) {
                report.error(
                        "CHUNK_ROW_COUNT",
                        uri,
                        "Chunk row count does not match its control-file range.");
            }
        }
    }

    private static long scan(
            URI uri,
            Schema expected,
            PhysicalReader reader,
            ValidationReport.Collector report,
            RowCheck rowCheck) {
        long rows = 0;
        try {
            try (BatchCursor cursor = reader.read(ReadRequest.builder(uri).build()).cursor()) {
                while (cursor.next()) {
                    RecordBatch batch = cursor.batch();
                    if (!sameSchema(expected, batch.schema())) {
                        report.error(
                                "SCHEMA_MISMATCH",
                                uri,
                                "Physical schema differs from GraphAr metadata: expected "
                                        + describe(expected)
                                        + ", got "
                                        + describe(batch.schema()));
                    }
                    for (int index = 0; index < batch.rowCount(); index++) {
                        if (rowCheck != null) rowCheck.check(batch.row(index), uri);
                        rows = Math.addExact(rows, 1);
                    }
                }
            }
            report.fileChecked();
            report.rowsChecked(rows);
        } catch (IOException | RuntimeException exception) {
            report.error(
                    "READ_FAILURE",
                    uri,
                    exception.getMessage() == null
                            ? exception.getClass().getSimpleName()
                            : exception.getMessage());
        }
        return rows;
    }

    private static Long readControl(Storage storage, URI uri, ValidationReport.Collector report) {
        if (!exists(storage, uri, report, ignored -> {})) return null;
        try {
            InputFile file = storage.inputFile(uri);
            if (file.size() != Long.BYTES) {
                report.error(
                        "CONTROL_SIZE",
                        uri,
                        "GraphAr control files must contain exactly one INT64.");
                return null;
            }
            ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            try (SeekableInput input = file.open()) {
                input.readFully(bytes);
            }
            long value = bytes.flip().getLong();
            if (value < 0) {
                report.error(
                        "CONTROL_NEGATIVE", uri, "GraphAr control values must be non-negative.");
                return null;
            }
            report.fileChecked();
            return value;
        } catch (IOException | RuntimeException exception) {
            report.error(
                    "CONTROL_READ",
                    uri,
                    exception.getMessage() == null
                            ? exception.getClass().getSimpleName()
                            : exception.getMessage());
            return null;
        }
    }

    private static boolean exists(
            Storage storage, URI uri, ValidationReport.Collector report, ExistsConsumer consumer) {
        boolean value = present(storage, uri, report);
        consumer.accept(value);
        if (!value)
            report.error("MISSING_FILE", uri, "Metadata-addressable GraphAr file is missing.");
        return value;
    }

    private static boolean present(Storage storage, URI uri, ValidationReport.Collector report) {
        try {
            return storage.exists(uri);
        } catch (IOException | RuntimeException exception) {
            report.error(
                    "STORAGE_FAILURE",
                    uri,
                    exception.getMessage() == null
                            ? exception.getClass().getSimpleName()
                            : exception.getMessage());
            return false;
        }
    }

    private static boolean sameSchema(Schema expected, Schema actual) {
        List<Field> expectedFields = expected.fields();
        List<Field> actualFields = actual.fields();
        if (expectedFields.size() != actualFields.size()) return false;
        for (int index = 0; index < expectedFields.size(); index++) {
            Field left = expectedFields.get(index);
            Field right = actualFields.get(index);
            if (!left.name().equals(right.name())
                    || !left.type().equals(right.type())
                    || left.nullable() != right.nullable()) return false;
        }
        return true;
    }

    private static String describe(Schema schema) {
        StringBuilder result = new StringBuilder("[");
        for (Field field : schema.fields()) {
            if (result.length() > 1) result.append(", ");
            result.append(field.name())
                    .append(':')
                    .append(field.type().kind())
                    .append(':')
                    .append(field.nullable());
        }
        return result.append(']').toString();
    }

    private static Schema propertySchema(PropertyGroup group) {
        java.util.ArrayList<Field> fields = new java.util.ArrayList<>();
        for (Property property : group) {
            ColumnType type = type(property.getDataType());
            if (property.getCardinality() != Cardinality.SINGLE
                    && type.kind() != ColumnType.Kind.LIST) type = ColumnType.listOf(type);
            fields.add(new Field(property.getName(), type, property.isNullable()));
        }
        return new Schema(fields);
    }

    private static Schema vertexSchema(PropertyGroup group) {
        List<Field> fields = new ArrayList<>();
        fields.add(VERTEX_INDEX_FIELD);
        fields.addAll(propertySchema(group).fields());
        return new Schema(fields);
    }

    private static ColumnType type(DataType dataType) {
        if (dataType.isList()) return ColumnType.listOf(type(dataType.getValueType()));
        if (dataType.equals(DataType.BOOL)) return ColumnType.of(ColumnType.Kind.BOOLEAN);
        if (dataType.equals(DataType.INT32)) return ColumnType.of(ColumnType.Kind.INT32);
        if (dataType.equals(DataType.INT64)) return ColumnType.of(ColumnType.Kind.INT64);
        if (dataType.equals(DataType.FLOAT)) return ColumnType.of(ColumnType.Kind.FLOAT32);
        if (dataType.equals(DataType.DOUBLE)) return ColumnType.of(ColumnType.Kind.FLOAT64);
        if (dataType.equals(DataType.STRING)) return ColumnType.of(ColumnType.Kind.STRING);
        if (dataType.equals(DataType.DATE)) return ColumnType.of(ColumnType.Kind.DATE);
        if (dataType.equals(DataType.TIMESTAMP))
            return ColumnType.of(ColumnType.Kind.TIMESTAMP_MILLIS);
        throw new IllegalArgumentException("Unsupported GraphAr property type: " + dataType);
    }

    private static long chunkSize(EdgeInfo edge, AdjListType layout) {
        return layout.getAlignedBy().equals("src")
                ? edge.getSrcChunkSize()
                : edge.getDstChunkSize();
    }

    private static long count(Long total, long chunkSize) {
        return total == null ? 0 : count(total.longValue(), chunkSize);
    }

    private static long count(long total, long chunkSize) {
        return total == 0 ? 0 : (total - 1) / chunkSize + 1;
    }

    private static URI directory(URI uri) {
        return URI.create(uri.toString().endsWith("/") ? uri.toString() : uri + "/");
    }

    private static URI absolute(URI root, URI uri) {
        return uri.isAbsolute() ? uri : root.resolve(uri);
    }

    private interface UriFactory {
        URI uri(long chunk);
    }

    private interface ExistsConsumer {
        void accept(boolean exists);
    }

    private interface RowCheck {
        void check(Row row, URI uri);
    }

    private static final class TopologyCheck implements RowCheck {
        private final AdjListType layout;
        private final long part;
        private final long partitionSize;
        private final Long sourceCount;
        private final Long destinationCount;
        private final ValidationReport.Collector report;
        private long previousAligned = -1;
        private long rows;

        private TopologyCheck(
                AdjListType layout,
                long part,
                long partitionSize,
                Long sourceCount,
                Long destinationCount,
                ValidationReport.Collector report) {
            this.layout = layout;
            this.part = part;
            this.partitionSize = partitionSize;
            this.sourceCount = sourceCount;
            this.destinationCount = destinationCount;
            this.report = report;
        }

        @Override
        public void check(Row row, URI uri) {
            Object source = row.value(0);
            Object destination = row.value(1);
            if (!(source instanceof Long) || !(destination instanceof Long)) {
                report.error("TOPOLOGY_TYPE", uri, "Topology IDs must be INT64 values.");
                return;
            }
            long src = (Long) source;
            long dst = (Long) destination;
            if (src < 0 || dst < 0)
                report.error("NEGATIVE_ID", uri, "Topology IDs must be non-negative.");
            if (sourceCount != null && src >= sourceCount)
                report.error("SOURCE_ID_BOUNDS", uri, "Source ID exceeds source vertex_count.");
            if (destinationCount != null && dst >= destinationCount)
                report.error(
                        "DESTINATION_ID_BOUNDS",
                        uri,
                        "Destination ID exceeds destination vertex_count.");
            long aligned = layout.getAlignedBy().equals("src") ? src : dst;
            if (aligned / partitionSize != part)
                report.error(
                        "PARTITION_ALIGNMENT",
                        uri,
                        "Topology row is stored in the wrong aligned partition.");
            if (layout.isOrdered() && aligned < previousAligned)
                report.error(
                        "ORDEREDNESS",
                        uri,
                        "Ordered adjacency is not non-decreasing by its aligned endpoint.");
            previousAligned = aligned;
            rows++;
        }
    }

    private static final class VertexIndexCheck implements RowCheck {
        private final long chunkSize;
        private final ValidationReport.Collector report;
        private URI currentUri;
        private long next;

        private VertexIndexCheck(long chunkSize, ValidationReport.Collector report) {
            this.chunkSize = chunkSize;
            this.report = report;
        }

        @Override
        public void check(Row row, URI uri) {
            if (!uri.equals(currentUri)) {
                currentUri = uri;
                next = 0;
            }
            Object value = row.value(0);
            if (!(value instanceof Long)) {
                report.error(
                        "VERTEX_INDEX_TYPE", uri, "_graphArVertexIndex must be an INT64 value.");
                return;
            }
            long chunk = chunk(uri);
            long expected = Math.addExact(Math.multiplyExact(chunk, chunkSize), next++);
            if ((Long) value != expected) {
                report.error(
                        "VERTEX_INDEX_SEQUENCE",
                        uri,
                        "_graphArVertexIndex must be sequential and begin at its chunk boundary.");
            }
        }

        private static long chunk(URI uri) {
            String name = uri.getPath().substring(uri.getPath().lastIndexOf('/') + 1);
            if (!name.startsWith("chunk")) return 0;
            try {
                return Long.parseLong(name.substring("chunk".length()));
            } catch (NumberFormatException exception) {
                return 0;
            }
        }
    }

    private static final class OffsetCheck implements RowCheck {
        private final long expectedRows;
        private final long terminal;
        private final ValidationReport.Collector report;
        private long rows;
        private long previous = -1;

        private OffsetCheck(long expectedRows, long terminal, ValidationReport.Collector report) {
            this.expectedRows = expectedRows;
            this.terminal = terminal;
            this.report = report;
        }

        @Override
        public void check(Row row, URI uri) {
            Object value = row.value(0);
            if (!(value instanceof Long)) {
                report.error("OFFSET_TYPE", uri, "Offsets must be INT64 values.");
                return;
            }
            long offset = (Long) value;
            if ((rows == 0 && offset != 0) || offset < previous || offset > terminal)
                report.error(
                        "OFFSET_MONOTONICITY",
                        uri,
                        "Offsets must start at zero, be monotonic, and not exceed edge_count.");
            previous = offset;
            rows++;
            if (rows == expectedRows && offset != terminal)
                report.error(
                        "OFFSET_TERMINAL", uri, "Final offset must equal partition edge_count.");
        }

        private void finish(URI uri) {
            if (rows == expectedRows && previous != terminal) {
                report.error(
                        "OFFSET_TERMINAL", uri, "Final offset must equal partition edge_count.");
            }
        }
    }
}
