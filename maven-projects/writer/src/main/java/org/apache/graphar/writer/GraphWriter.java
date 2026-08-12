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

package org.apache.graphar.writer;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.graphar.core.ChunkMath;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.Cardinality;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.PhysicalWriter;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Row;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.storage.PositionOutput;
import org.apache.graphar.storage.Storage;

/** Writes the first pure-Java GraphAr Parquet vertex and ordered-source topology vertical. */
public final class GraphWriter {
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

    private final GraphInfo graphInfo;
    private final URI datasetRoot;
    private final Storage storage;
    private final PhysicalWriter physicalWriter;
    private final WriteMode writeMode;

    /** Creates a writer using {@link WriteMode#CREATE_NEW} for every GraphAr output. */
    public GraphWriter(
            GraphInfo graphInfo, URI datasetRoot, Storage storage, PhysicalWriter physicalWriter) {
        this(graphInfo, datasetRoot, storage, physicalWriter, WriteMode.CREATE_NEW);
    }

    /** Creates a writer with one explicit target-existence policy for every GraphAr output. */
    public GraphWriter(
            GraphInfo graphInfo,
            URI datasetRoot,
            Storage storage,
            PhysicalWriter physicalWriter,
            WriteMode writeMode) {
        this.graphInfo = Objects.requireNonNull(graphInfo, "Graph info cannot be null.");
        if (!graphInfo.isValidated()) {
            throw new IllegalArgumentException("Graph info must be valid before writing data.");
        }
        this.datasetRoot = directory(datasetRoot);
        this.storage = Objects.requireNonNull(storage, "Storage cannot be null.");
        this.physicalWriter =
                Objects.requireNonNull(physicalWriter, "Physical writer cannot be null.");
        this.writeMode = Objects.requireNonNull(writeMode, "Write mode cannot be null.");
    }

    /** Returns the normalized root under which relative GraphAr metadata URIs are written. */
    public URI datasetRoot() {
        return datasetRoot;
    }

    /**
     * Streams one Parquet vertex property group into GraphAr vertex chunks and writes its
     * vertex-count control file after all chunks succeed. The cursor is closed by this method.
     */
    public long writeVertexPropertyGroup(
            VertexInfo vertexInfo, PropertyGroup propertyGroup, BatchCursor source)
            throws IOException {
        Objects.requireNonNull(vertexInfo, "Vertex info cannot be null.");
        Objects.requireNonNull(propertyGroup, "Property group cannot be null.");
        Objects.requireNonNull(source, "Vertex source cannot be null.");
        requireVertexGroup(vertexInfo, propertyGroup);
        Schema schema = schema(propertyGroup);
        long count = 0;
        long chunk = 0;
        List<Row> rows = new ArrayList<>();
        try {
            while (source.next()) {
                RecordBatch batch =
                        Objects.requireNonNull(source.batch(), "batch cursor returned null");
                requireSchema(schema, batch.schema());
                for (int index = 0; index < batch.rowCount(); index++) {
                    rows.add(copyRow(batch.row(index), schema));
                    count = Math.addExact(count, 1);
                    if (rows.size() == vertexInfo.getChunkSize()) {
                        writeRows(
                                vertexInfo.getPropertyGroupChunkUri(propertyGroup, chunk++),
                                schema,
                                rows);
                        rows = new ArrayList<>();
                    }
                }
            }
        } finally {
            source.close();
        }
        if (!rows.isEmpty()) {
            writeRows(vertexInfo.getPropertyGroupChunkUri(propertyGroup, chunk), schema, rows);
        }
        writeLong(vertexInfo.getVerticesNumFileUri(), count);
        return count;
    }

    /**
     * Writes validated source-sorted topology as GraphAr ordered-by-source offsets, adjacency
     * chunks, partition edge counts, and source vertex count. The supplied list is intentionally
     * bounded in this MVP so ordering is validated before any output is published.
     */
    public long writeOrderedSourceTopology(
            EdgeInfo edgeInfo, long sourceVertexCount, List<TopologyEdge> edges)
            throws IOException {
        Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        Objects.requireNonNull(edges, "Topology edges cannot be null.");
        if (!edgeInfo.hasAdjListType(AdjListType.ordered_by_source)) {
            throw new IllegalArgumentException(
                    "Edge info must declare ordered_by_source adjacency.");
        }
        if (sourceVertexCount < 0) {
            throw new IllegalArgumentException("Source vertex count must be non-negative.");
        }
        requireParquet(
                edgeInfo.getAdjacentList(AdjListType.ordered_by_source).getFileType(), "adjacency");
        validateEdges(sourceVertexCount, edges);
        long partitionCount = ChunkMath.chunkCount(sourceVertexCount, edgeInfo.getSrcChunkSize());
        int nextEdge = 0;
        long total = 0;
        for (long partition = 0; partition < partitionCount; partition++) {
            long partitionStart = Math.multiplyExact(partition, edgeInfo.getSrcChunkSize());
            long verticesInPartition =
                    Math.min(edgeInfo.getSrcChunkSize(), sourceVertexCount - partitionStart);
            List<Row> offsets = new ArrayList<>();
            offsets.add(new ArrayRow(new Object[] {0L}));
            List<Row> adjacency = new ArrayList<>();
            long partitionEdges = 0;
            long edgeChunk = 0;
            for (long localVertex = 0; localVertex < verticesInPartition; localVertex++) {
                long source = partitionStart + localVertex;
                while (nextEdge < edges.size() && edges.get(nextEdge).source() == source) {
                    TopologyEdge edge = edges.get(nextEdge++);
                    adjacency.add(new ArrayRow(new Object[] {edge.source(), edge.destination()}));
                    partitionEdges = Math.addExact(partitionEdges, 1);
                    total = Math.addExact(total, 1);
                    if (adjacency.size() == edgeInfo.getChunkSize()) {
                        writeRows(
                                edgeInfo.getAdjacentListChunkUri(
                                        AdjListType.ordered_by_source, partition, edgeChunk++),
                                TOPOLOGY_SCHEMA,
                                adjacency);
                        adjacency = new ArrayList<>();
                    }
                }
                offsets.add(new ArrayRow(new Object[] {partitionEdges}));
            }
            if (!adjacency.isEmpty()) {
                writeRows(
                        edgeInfo.getAdjacentListChunkUri(
                                AdjListType.ordered_by_source, partition, edgeChunk),
                        TOPOLOGY_SCHEMA,
                        adjacency);
            }
            writeRows(
                    edgeInfo.getOffsetChunkUri(AdjListType.ordered_by_source, partition),
                    OFFSET_SCHEMA,
                    offsets);
            writeLong(
                    edgeInfo.getEdgesNumFileUri(AdjListType.ordered_by_source, partition),
                    partitionEdges);
        }
        if (nextEdge != edges.size()) {
            throw new IllegalStateException(
                    "Validated topology edges were not assigned to a source partition.");
        }
        writeLong(edgeInfo.getVerticesNumFileUri(AdjListType.ordered_by_source), sourceVertexCount);
        return total;
    }

    /**
     * Writes referenced vertex and edge YAML files first, then the graph YAML last as the
     * graph-root publication marker. Graph metadata remains an explicit caller-owned input.
     */
    public void writeMetadata(URI graphYamlUri) throws IOException {
        URI graphUri = absolute(graphYamlUri);
        for (VertexInfo vertexInfo : graphInfo.getVertexInfos()) {
            writeUtf8(graphUri.resolve(graphInfo.getStoreUri(vertexInfo)), vertexInfo.dump());
        }
        for (EdgeInfo edgeInfo : graphInfo.getEdgeInfos()) {
            writeUtf8(graphUri.resolve(graphInfo.getStoreUri(edgeInfo)), edgeInfo.dump());
        }
        writeUtf8(graphUri, graphInfo.dump(graphUri));
    }

    private void writeRows(URI uri, Schema schema, List<Row> rows) throws IOException {
        physicalWriter.write(
                new WriteRequest(absolute(uri), schema, writeMode),
                new SingleBatchCursor(new ListRecordBatch(schema, rows)));
    }

    private void writeLong(URI uri, long value) throws IOException {
        ByteBuffer bytes =
                ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value);
        try (PositionOutput output = output(absolute(uri))) {
            output.write(bytes.array());
        }
    }

    private void writeUtf8(URI uri, String value) throws IOException {
        try (PositionOutput output = output(uri)) {
            output.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private PositionOutput output(URI uri) throws IOException {
        return writeMode == WriteMode.CREATE_NEW
                ? storage.outputFile(uri).create()
                : storage.outputFile(uri).createOrOverwrite();
    }

    private URI absolute(URI uri) {
        return uri.isAbsolute() ? uri : datasetRoot.resolve(uri);
    }

    private static URI directory(URI uri) {
        Objects.requireNonNull(uri, "Dataset root cannot be null.");
        String value = uri.toString();
        return URI.create(value.endsWith("/") ? value : value + "/");
    }

    private static void requireVertexGroup(VertexInfo vertexInfo, PropertyGroup propertyGroup) {
        if (!vertexInfo.hasPropertyGroup(propertyGroup)) {
            throw new IllegalArgumentException("Property group does not belong to vertex info.");
        }
        requireParquet(propertyGroup.getFileType(), "vertex property group");
    }

    private static void requireParquet(FileType fileType, String kind) {
        if (fileType != FileType.PARQUET) {
            throw new IllegalArgumentException("Writer MVP supports Parquet " + kind + " only.");
        }
    }

    private static Schema schema(PropertyGroup propertyGroup) {
        List<Field> fields = new ArrayList<>();
        for (Property property : propertyGroup) {
            if (property.getCardinality() != Cardinality.SINGLE
                    || property.getDataType().isList()) {
                throw new IllegalArgumentException(
                        "Writer MVP supports single-value properties only.");
            }
            fields.add(
                    new Field(
                            property.getName(),
                            type(property.getDataType()),
                            property.isNullable()));
        }
        return new Schema(fields);
    }

    private static ColumnType type(DataType dataType) {
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

    private static void validateEdges(long sourceVertexCount, List<TopologyEdge> edges) {
        long previousSource = -1;
        for (TopologyEdge edge : edges) {
            if (edge == null || edge.source() < 0 || edge.destination() < 0) {
                throw new IllegalArgumentException("Topology IDs must be non-negative.");
            }
            if (edge.source() >= sourceVertexCount) {
                throw new IllegalArgumentException(
                        "Topology source exceeds declared source vertex count.");
            }
            if (edge.source() < previousSource) {
                throw new IllegalArgumentException(
                        "ordered_by_source topology must be source-sorted.");
            }
            previousSource = edge.source();
        }
    }

    private static void requireSchema(Schema expected, Schema actual) {
        if (actual == null || expected.fields().size() != actual.fields().size()) {
            throw new IllegalArgumentException(
                    "Vertex source schema does not match property group.");
        }
        for (int index = 0; index < expected.fields().size(); index++) {
            Field left = expected.fields().get(index);
            Field right = actual.fields().get(index);
            if (!left.name().equals(right.name())
                    || !left.type().equals(right.type())
                    || left.nullable() != right.nullable()) {
                throw new IllegalArgumentException(
                        "Vertex source schema does not match property group.");
            }
        }
    }

    private static Row copyRow(Row source, Schema schema) {
        Object[] values = new Object[schema.fields().size()];
        for (int index = 0; index < values.length; index++) values[index] = source.value(index);
        return new ArrayRow(values);
    }

    private static final class ArrayRow implements Row {
        private final Object[] values;

        private ArrayRow(Object[] values) {
            this.values = values;
        }

        @Override
        public Object value(int columnIndex) {
            return values[columnIndex];
        }
    }

    private static final class ListRecordBatch implements RecordBatch {
        private final Schema schema;
        private final List<Row> rows;

        private ListRecordBatch(Schema schema, List<Row> rows) {
            this.schema = schema;
            this.rows = List.copyOf(rows);
        }

        @Override
        public Schema schema() {
            return schema;
        }

        @Override
        public int rowCount() {
            return rows.size();
        }

        @Override
        public Row row(int index) {
            return rows.get(index);
        }
    }

    private static final class SingleBatchCursor implements BatchCursor {
        private final RecordBatch batch;
        private boolean available = true;

        private SingleBatchCursor(RecordBatch batch) {
            this.batch = batch;
        }

        @Override
        public boolean next() {
            boolean result = available;
            available = false;
            return result;
        }

        @Override
        public RecordBatch batch() {
            if (available) throw new IllegalStateException("Call next() before batch().");
            return batch;
        }

        @Override
        public void close() {}
    }
}
