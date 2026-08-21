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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Stream;
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

/** Writes GraphAr Parquet vertex property groups and all declared adjacency layouts. */
public final class GraphWriter {
    private static final int MAX_MERGE_INPUTS = 32;
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
        Schema sourceSchema = schema(propertyGroup);
        Schema outputSchema = vertexSchema(propertyGroup);
        long count = 0;
        long chunk = 0;
        List<Row> rows = new ArrayList<>();
        try {
            while (source.next()) {
                RecordBatch batch =
                        Objects.requireNonNull(source.batch(), "batch cursor returned null");
                requireSchema(sourceSchema, batch.schema());
                for (int index = 0; index < batch.rowCount(); index++) {
                    Row row = batch.row(index);
                    validatePropertyCardinality(propertyGroup, row);
                    rows.add(vertexRow(count, row, sourceSchema));
                    count = Math.addExact(count, 1);
                    if (rows.size() == vertexInfo.getChunkSize()) {
                        writeRows(
                                vertexInfo.getPropertyGroupChunkUri(propertyGroup, chunk++),
                                outputSchema,
                                rows);
                        rows = new ArrayList<>();
                    }
                }
            }
        } finally {
            source.close();
        }
        if (!rows.isEmpty()) {
            writeRows(
                    vertexInfo.getPropertyGroupChunkUri(propertyGroup, chunk), outputSchema, rows);
        }
        writeLong(vertexInfo.getVerticesNumFileUri(), count);
        return count;
    }

    /**
     * Writes one vertex property chunk of a group, leaving every other chunk of that group
     * byte-identical.
     *
     * <p>A projection under continuous ingest grows by vertices that sort past everything it holds,
     * and those vertices land in the partition the projection stopped at and in the partitions
     * after it. Rewriting the whole group to publish them costs the size of the group rather than
     * the size of the arrival, which is the cost this method exists to avoid: chunk {@code
     * partition} is the only output it touches.
     *
     * <p>The vertex indices written are {@code partition * chunkSize} upwards, so the caller
     * supplies exactly the rows of that partition, in vertex order. A short chunk is accepted
     * because the last partition of a group is short by definition; writing one in the middle of a
     * group is the caller declaring the group ends there. The vertex-count control file is left
     * untouched - how many vertices a group holds is a property of the group, not of one chunk - so
     * a growing projection publishes its new count with {@link #writeVertexCount} once every chunk
     * it added has been written.
     *
     * @return the number of rows written into the partition
     * @throws IllegalStateException when this writer is not in {@link WriteMode#OVERWRITE}, since a
     *     partition that a projection stopped inside already exists
     * @throws IllegalArgumentException when the source holds more rows than the chunk size
     */
    public long writeVertexPartition(
            VertexInfo vertexInfo, PropertyGroup propertyGroup, long partition, BatchCursor source)
            throws IOException {
        Objects.requireNonNull(vertexInfo, "Vertex info cannot be null.");
        Objects.requireNonNull(propertyGroup, "Property group cannot be null.");
        Objects.requireNonNull(source, "Vertex source cannot be null.");
        requireVertexGroup(vertexInfo, propertyGroup);
        if (writeMode != WriteMode.OVERWRITE) {
            throw new IllegalStateException(
                    "Rewriting an existing vertex partition requires WriteMode.OVERWRITE.");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("Vertex partition must be non-negative.");
        }
        Schema sourceSchema = schema(propertyGroup);
        Schema outputSchema = vertexSchema(propertyGroup);
        long chunkSize = vertexInfo.getChunkSize();
        long firstVertex = Math.multiplyExact(partition, chunkSize);
        List<Row> rows = new ArrayList<>();
        try {
            while (source.next()) {
                RecordBatch batch =
                        Objects.requireNonNull(source.batch(), "batch cursor returned null");
                requireSchema(sourceSchema, batch.schema());
                for (int index = 0; index < batch.rowCount(); index++) {
                    Row row = batch.row(index);
                    validatePropertyCardinality(propertyGroup, row);
                    if (rows.size() == chunkSize) {
                        throw new IllegalArgumentException(
                                "Vertex partition source exceeds the chunk size of " + chunkSize);
                    }
                    rows.add(vertexRow(firstVertex + rows.size(), row, sourceSchema));
                }
            }
        } finally {
            source.close();
        }
        writeRows(
                vertexInfo.getPropertyGroupChunkUri(propertyGroup, partition), outputSchema, rows);
        return rows.size();
    }

    /**
     * Publishes how many vertices a type holds, without writing any of them.
     *
     * <p>This is the second half of growing a vertex type one partition at a time: the partitions
     * carry the rows, and this carries the count that makes them readable. Publishing the count
     * before the last partition is written would expose vertices whose properties are not there
     * yet, so callers write every partition first.
     */
    public void writeVertexCount(VertexInfo vertexInfo, long vertexCount) throws IOException {
        Objects.requireNonNull(vertexInfo, "Vertex info cannot be null.");
        if (vertexCount < 0) {
            throw new IllegalArgumentException("Vertex count must be non-negative.");
        }
        writeLong(vertexInfo.getVerticesNumFileUri(), vertexCount);
    }

    /**
     * Publishes how many source vertices an adjacency layout is aligned to.
     *
     * <p>{@link #writeEdgeLayout} stamps this once it has written every partition, so a caller that
     * grows a layout partition by partition has no way to move it. Without the move the reader
     * keeps aligning to the old count and the partitions added past it are never visited, so this
     * is the edge-side counterpart of {@link #writeVertexCount}: written last, after the partitions
     * that the new count makes reachable.
     */
    public void writeEdgeVertexCount(EdgeInfo edgeInfo, AdjListType layout, long alignedVertexCount)
            throws IOException {
        Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        Objects.requireNonNull(layout, "Adjacency layout cannot be null.");
        if (!edgeInfo.hasAdjListType(layout)) {
            throw new IllegalArgumentException(
                    "Edge info does not declare adjacency layout: " + layout);
        }
        if (alignedVertexCount < 0) {
            throw new IllegalArgumentException("Aligned vertex count must be non-negative.");
        }
        writeLong(edgeInfo.getVerticesNumFileUri(layout), alignedVertexCount);
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
     * Writes topology and every declared edge property group in exactly the same physical row
     * order. Ordered layouts receive CSR/CSC offsets; unordered layouts are aligned COO partitions
     * and intentionally have no offset files.
     */
    public long writeEdgeLayout(
            EdgeInfo edgeInfo,
            AdjListType layout,
            long alignedVertexCount,
            List<EdgeRecord> records)
            throws IOException {
        return writeEdgeLayout(
                        edgeInfo,
                        layout,
                        alignedVertexCount,
                        (Iterable<EdgeRecord>) records,
                        EdgeWriteOptions.defaults())
                .edgeCount();
    }

    /**
     * Streams an edge source through bounded external sort runs and publishes topology and edge
     * properties in their shared physical row order. The source is consumed exactly once; the
     * implementation never retains more than {@link EdgeWriteOptions#maxRecordsInMemory()} edge
     * records in heap while ordering a partition.
     */
    public EdgeWriteStats writeEdgeLayout(
            EdgeInfo edgeInfo,
            AdjListType layout,
            long alignedVertexCount,
            Iterable<EdgeRecord> records,
            EdgeWriteOptions options)
            throws IOException {
        Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        Objects.requireNonNull(layout, "Adjacency layout cannot be null.");
        Objects.requireNonNull(records, "Edge records cannot be null.");
        Objects.requireNonNull(options, "Edge write options cannot be null.");
        if (!edgeInfo.hasAdjListType(layout)) {
            throw new IllegalArgumentException(
                    "Edge info does not declare adjacency layout: " + layout);
        }
        if (alignedVertexCount < 0) {
            throw new IllegalArgumentException("Aligned vertex count must be non-negative.");
        }
        requireParquet(edgeInfo.getAdjacentList(layout).getFileType(), "adjacency");
        long vertexChunkSize =
                layout.getAlignedBy().equals("src")
                        ? edgeInfo.getSrcChunkSize()
                        : edgeInfo.getDstChunkSize();
        long partitionCount = ChunkMath.chunkCount(alignedVertexCount, vertexChunkSize);
        List<PropertyGroup> propertyGroups = edgePropertyGroups(edgeInfo);
        EdgeRecordCodec codec = new EdgeRecordCodec(edgeProperties(edgeInfo));
        Path workDirectory = Files.createTempDirectory("graphar-edge-write-");
        long spillRuns = 0;
        int peakRecordsBuffered = 0;
        long total = 0;
        try {
            spillInput(
                    records,
                    codec,
                    layout,
                    alignedVertexCount,
                    vertexChunkSize,
                    workDirectory,
                    OptionalLong.empty());
            for (long partition = 0; partition < partitionCount; partition++) {
                PartitionOutcome outcome =
                        writePartition(
                                edgeInfo,
                                layout,
                                alignedVertexCount,
                                vertexChunkSize,
                                partition,
                                propertyGroups,
                                codec,
                                workDirectory,
                                options.maxRecordsInMemory(),
                                spillRuns);
                total = Math.addExact(total, outcome.edgeCount);
                spillRuns = Math.addExact(spillRuns, outcome.spillRuns);
                peakRecordsBuffered = Math.max(peakRecordsBuffered, outcome.peakRecords);
            }
            writeLong(edgeInfo.getVerticesNumFileUri(layout), alignedVertexCount);
            return new EdgeWriteStats(total, partitionCount, spillRuns, peakRecordsBuffered);
        } finally {
            deleteTree(workDirectory);
        }
    }

    /**
     * Rewrites one vertex-aligned partition of an existing adjacency layout from the complete set
     * of edges that partition is to hold, and publishes nothing outside it.
     *
     * <p>GraphAr keeps offsets, the adjacency chunk sequence, and the edge count of a partition
     * inside that partition, so recomputing one partition leaves every other partition of the same
     * layout byte-identical. That is what makes folding accumulated patches for a single chunk back
     * into the base a bounded operation instead of a full rebuild of the layout.
     *
     * <p>The source is drained into bounded local runs in full before the first output chunk is
     * written, which is what allows the source to be a cursor over the very partition being
     * rewritten. The layout's vertex-count control file is left untouched, because rewriting a
     * partition cannot change how many vertices the layout is aligned to.
     *
     * @throws IllegalStateException when this writer is not in {@link WriteMode#OVERWRITE}, since
     *     every output of an existing partition already exists
     * @throws IllegalArgumentException when a record's aligned endpoint falls outside {@code
     *     partition}
     */
    public EdgeWriteStats writeEdgePartition(
            EdgeInfo edgeInfo,
            AdjListType layout,
            long alignedVertexCount,
            long partition,
            Iterable<EdgeRecord> records,
            EdgeWriteOptions options)
            throws IOException {
        Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        Objects.requireNonNull(layout, "Adjacency layout cannot be null.");
        Objects.requireNonNull(records, "Edge records cannot be null.");
        Objects.requireNonNull(options, "Edge write options cannot be null.");
        if (writeMode != WriteMode.OVERWRITE) {
            throw new IllegalStateException(
                    "Rewriting an existing edge partition requires WriteMode.OVERWRITE.");
        }
        if (!edgeInfo.hasAdjListType(layout)) {
            throw new IllegalArgumentException(
                    "Edge info does not declare adjacency layout: " + layout);
        }
        if (alignedVertexCount < 0) {
            throw new IllegalArgumentException("Aligned vertex count must be non-negative.");
        }
        requireParquet(edgeInfo.getAdjacentList(layout).getFileType(), "adjacency");
        long vertexChunkSize =
                layout.getAlignedBy().equals("src")
                        ? edgeInfo.getSrcChunkSize()
                        : edgeInfo.getDstChunkSize();
        long partitionCount = ChunkMath.chunkCount(alignedVertexCount, vertexChunkSize);
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException(
                    "Edge partition is outside this adjacency layout: " + partition);
        }
        List<PropertyGroup> propertyGroups = edgePropertyGroups(edgeInfo);
        EdgeRecordCodec codec = new EdgeRecordCodec(edgeProperties(edgeInfo));
        Path workDirectory = Files.createTempDirectory("graphar-edge-partition-");
        try {
            spillInput(
                    records,
                    codec,
                    layout,
                    alignedVertexCount,
                    vertexChunkSize,
                    workDirectory,
                    OptionalLong.of(partition));
            PartitionOutcome outcome =
                    writePartition(
                            edgeInfo,
                            layout,
                            alignedVertexCount,
                            vertexChunkSize,
                            partition,
                            propertyGroups,
                            codec,
                            workDirectory,
                            options.maxRecordsInMemory(),
                            0);
            return new EdgeWriteStats(outcome.edgeCount, 1, outcome.spillRuns, outcome.peakRecords);
        } finally {
            deleteTree(workDirectory);
        }
    }

    private PartitionOutcome writePartition(
            EdgeInfo edgeInfo,
            AdjListType layout,
            long alignedVertexCount,
            long vertexChunkSize,
            long partition,
            List<PropertyGroup> propertyGroups,
            EdgeRecordCodec codec,
            Path workDirectory,
            int maxRecordsInMemory,
            long runSequence)
            throws IOException {
        Path input = partitionPath(workDirectory, partition);
        RunSet runSet =
                sortedRuns(input, codec, layout, maxRecordsInMemory, workDirectory, partition);
        List<Path> runs = runSet.paths;
        long spillRuns = runs.size();
        MergeSet merged =
                compactRuns(
                        runs,
                        codec,
                        layout,
                        workDirectory,
                        partition,
                        Math.addExact(runSequence, spillRuns));
        runs = merged.paths;
        spillRuns = Math.addExact(spillRuns, merged.createdRuns);
        PartitionEdgeWriter partitionWriter =
                new PartitionEdgeWriter(
                        edgeInfo,
                        layout,
                        partition,
                        Math.multiplyExact(partition, vertexChunkSize),
                        Math.min(
                                alignedVertexCount,
                                Math.multiplyExact(partition + 1, vertexChunkSize)),
                        propertyGroups,
                        codec,
                        workDirectory);
        mergeRuns(runs, codec, layout, partitionWriter::accept);
        long edgeCount = partitionWriter.finish();
        delete(input);
        for (Path run : runs) {
            delete(run);
        }
        return new PartitionOutcome(edgeCount, spillRuns, runSet.peakRecords);
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

    private static void spillInput(
            Iterable<EdgeRecord> records,
            EdgeRecordCodec codec,
            AdjListType layout,
            long alignedVertexCount,
            long vertexChunkSize,
            Path workDirectory,
            OptionalLong onlyPartition)
            throws IOException {
        try (PartitionSpillWriter partitions = new PartitionSpillWriter(workDirectory, codec)) {
            for (EdgeRecord record : records) {
                validateRecord(codec, layout, alignedVertexCount, record);
                long partition = Math.floorDiv(aligned(record, layout), vertexChunkSize);
                if (onlyPartition.isPresent() && onlyPartition.getAsLong() != partition) {
                    throw new IllegalArgumentException(
                            "Edge record belongs to partition "
                                    + partition
                                    + " and not to the rewritten partition "
                                    + onlyPartition.getAsLong()
                                    + ".");
                }
                partitions.write(partition, record);
            }
        }
    }

    private static RunSet sortedRuns(
            Path input,
            EdgeRecordCodec codec,
            AdjListType layout,
            int maxRecords,
            Path workDirectory,
            long partition)
            throws IOException {
        if (!Files.exists(input)) {
            return new RunSet(List.of(), 0);
        }
        if (!layout.isOrdered()) {
            return new RunSet(List.of(input), 0);
        }
        List<Path> runs = new ArrayList<>();
        int peakRecords = 0;
        try (EdgeRecordInput stream = new EdgeRecordInput(input, codec)) {
            while (true) {
                List<EdgeRecord> records = new ArrayList<>(maxRecords);
                while (records.size() < maxRecords) {
                    EdgeRecord record = stream.next();
                    if (record == null) {
                        break;
                    }
                    records.add(record);
                }
                if (records.isEmpty()) {
                    break;
                }
                peakRecords = Math.max(peakRecords, records.size());
                records.sort(Comparator.comparingLong(record -> aligned(record, layout)));
                Path run =
                        workDirectory.resolve(
                                "partition-" + partition + "-run-" + runs.size() + ".bin");
                try (DataOutputStream output = output(run)) {
                    for (EdgeRecord record : records) {
                        codec.write(output, record);
                    }
                }
                runs.add(run);
            }
        }
        delete(input);
        return new RunSet(List.copyOf(runs), peakRecords);
    }

    private static void mergeRuns(
            List<Path> runs, EdgeRecordCodec codec, AdjListType layout, EdgeConsumer consumer)
            throws IOException {
        if (runs.size() > MAX_MERGE_INPUTS) {
            throw new IllegalArgumentException("Merge fan-in exceeds the bounded writer limit.");
        }
        List<EdgeRecordInput> inputs = new ArrayList<>();
        PriorityQueue<RunHead> heads =
                new PriorityQueue<>(
                        Comparator.comparingLong((RunHead head) -> aligned(head.record, layout))
                                .thenComparingInt(head -> head.run));
        try {
            for (int index = 0; index < runs.size(); index++) {
                EdgeRecordInput input = new EdgeRecordInput(runs.get(index), codec);
                inputs.add(input);
                EdgeRecord record = input.next();
                if (record != null) {
                    heads.add(new RunHead(index, record));
                }
            }
            while (!heads.isEmpty()) {
                RunHead head = heads.remove();
                consumer.accept(head.record);
                EdgeRecord next = inputs.get(head.run).next();
                if (next != null) {
                    heads.add(new RunHead(head.run, next));
                }
            }
        } finally {
            IOException closeFailure = null;
            for (EdgeRecordInput input : inputs) {
                try {
                    input.close();
                } catch (IOException exception) {
                    if (closeFailure == null) {
                        closeFailure = exception;
                    } else {
                        closeFailure.addSuppressed(exception);
                    }
                }
            }
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static MergeSet compactRuns(
            List<Path> initial,
            EdgeRecordCodec codec,
            AdjListType layout,
            Path workDirectory,
            long partition,
            long runSequence)
            throws IOException {
        List<Path> current = new ArrayList<>(initial);
        long created = 0;
        long sequence = runSequence;
        while (current.size() > MAX_MERGE_INPUTS) {
            List<Path> next = new ArrayList<>();
            for (int start = 0; start < current.size(); start += MAX_MERGE_INPUTS) {
                List<Path> group =
                        current.subList(
                                Math.min(start, current.size()),
                                Math.min(start + MAX_MERGE_INPUTS, current.size()));
                Path merged =
                        workDirectory.resolve(
                                "partition-" + partition + "-merge-" + sequence++ + ".bin");
                try (DataOutputStream output = output(merged)) {
                    mergeRuns(group, codec, layout, record -> codec.write(output, record));
                }
                for (Path path : group) {
                    delete(path);
                }
                next.add(merged);
                created++;
            }
            current = next;
        }
        return new MergeSet(List.copyOf(current), created);
    }

    private void writeEdgeChunkFromSpill(
            EdgeInfo edgeInfo,
            AdjListType layout,
            long partition,
            long edgeChunk,
            Path chunk,
            long count,
            EdgeRecordCodec codec,
            List<PropertyGroup> propertyGroups)
            throws IOException {
        physicalWriter.write(
                new WriteRequest(
                        absolute(edgeInfo.getAdjacentListChunkUri(layout, partition, edgeChunk)),
                        TOPOLOGY_SCHEMA,
                        writeMode),
                new EdgeFileBatchCursor(
                        chunk,
                        count,
                        codec,
                        TOPOLOGY_SCHEMA,
                        record ->
                                new ArrayRow(
                                        new Object[] {record.source(), record.destination()})));
        for (PropertyGroup group : propertyGroups) {
            Schema schema = schema(group);
            physicalWriter.write(
                    new WriteRequest(
                            absolute(
                                    edgeInfo.getPropertyGroupChunkUri(
                                            group, layout, partition, edgeChunk)),
                            schema,
                            writeMode),
                    new EdgeFileBatchCursor(
                            chunk, count, codec, schema, record -> propertyRow(group, record)));
        }
    }

    private static Row propertyRow(PropertyGroup group, EdgeRecord record) {
        Object[] values = new Object[group.size()];
        int index = 0;
        for (Property property : group) {
            values[index++] = record.properties().get(property.getName());
        }
        return new ArrayRow(values);
    }

    private static Path partitionPath(Path workDirectory, long partition) {
        return workDirectory.resolve("partition-" + partition + ".bin");
    }

    private static DataOutputStream output(Path path) throws IOException {
        return new DataOutputStream(
                new BufferedOutputStream(
                        Files.newOutputStream(
                                path,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.TRUNCATE_EXISTING,
                                StandardOpenOption.WRITE)));
    }

    private static void delete(Path path) throws IOException {
        Files.deleteIfExists(path);
    }

    private static void deleteTree(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            Iterator<Path> iterator = paths.sorted(Comparator.reverseOrder()).iterator();
            IOException failure = null;
            while (iterator.hasNext()) {
                try {
                    Files.deleteIfExists(iterator.next());
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
            throw new IllegalArgumentException("Graph writer supports Parquet " + kind + " only.");
        }
    }

    private static Schema schema(PropertyGroup propertyGroup) {
        List<Field> fields = new ArrayList<>();
        for (Property property : propertyGroup) {
            ColumnType type = type(property.getDataType());
            if (property.getCardinality() != Cardinality.SINGLE
                    && type.kind() != ColumnType.Kind.LIST) {
                type = ColumnType.listOf(type);
            }
            fields.add(new Field(property.getName(), type, property.isNullable()));
        }
        return new Schema(fields);
    }

    private static Schema vertexSchema(PropertyGroup propertyGroup) {
        List<Field> fields = new ArrayList<>();
        fields.add(VERTEX_INDEX_FIELD);
        fields.addAll(schema(propertyGroup).fields());
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

    private static long aligned(EdgeRecord record, AdjListType layout) {
        return layout.getAlignedBy().equals("src") ? record.source() : record.destination();
    }

    private static List<PropertyGroup> edgePropertyGroups(EdgeInfo edgeInfo) {
        List<PropertyGroup> groups = new ArrayList<>();
        for (int index = 0; index < edgeInfo.getPropertyGroupNum(); index++) {
            PropertyGroup group = edgeInfo.getPropertyGroupByIndex(index);
            requireParquet(group.getFileType(), "edge property group");
            groups.add(group);
        }
        return List.copyOf(groups);
    }

    private static List<Property> edgeProperties(EdgeInfo edgeInfo) {
        List<Property> properties = new ArrayList<>();
        for (int index = 0; index < edgeInfo.getPropertyGroupNum(); index++) {
            for (Property property : edgeInfo.getPropertyGroupByIndex(index)) {
                properties.add(property);
            }
        }
        return List.copyOf(properties);
    }

    private static void validateRecord(
            EdgeRecordCodec codec, AdjListType layout, long alignedVertexCount, EdgeRecord record) {
        if (record == null || record.source() < 0 || record.destination() < 0) {
            throw new IllegalArgumentException("Topology IDs must be non-negative.");
        }
        if (aligned(record, layout) >= alignedVertexCount) {
            throw new IllegalArgumentException(
                    "Aligned edge endpoint exceeds declared vertex count.");
        }
        if (!record.properties().keySet().equals(codec.propertyNames)) {
            throw new IllegalArgumentException(
                    "Edge record properties do not match EdgeInfo property groups.");
        }
    }

    private void writeEdgeChunk(
            EdgeInfo edgeInfo,
            AdjListType layout,
            long partition,
            long edgeChunk,
            List<EdgeRecord> records,
            List<PropertyGroup> propertyGroups)
            throws IOException {
        List<Row> topology = new ArrayList<>(records.size());
        for (EdgeRecord record : records) {
            topology.add(new ArrayRow(new Object[] {record.source(), record.destination()}));
        }
        writeRows(
                edgeInfo.getAdjacentListChunkUri(layout, partition, edgeChunk),
                TOPOLOGY_SCHEMA,
                topology);
        for (PropertyGroup propertyGroup : propertyGroups) {
            Schema schema = schema(propertyGroup);
            List<Row> properties = new ArrayList<>(records.size());
            for (EdgeRecord record : records) {
                Object[] values = new Object[propertyGroup.size()];
                int index = 0;
                for (Property property : propertyGroup) {
                    values[index++] = record.properties().get(property.getName());
                }
                properties.add(new ArrayRow(values));
            }
            writeRows(
                    edgeInfo.getPropertyGroupChunkUri(propertyGroup, layout, partition, edgeChunk),
                    schema,
                    properties);
        }
    }

    private static void validateRecords(
            EdgeInfo edgeInfo,
            AdjListType layout,
            long alignedVertexCount,
            List<EdgeRecord> records) {
        Set<String> expectedProperties = new HashSet<>();
        for (int index = 0; index < edgeInfo.getPropertyGroupNum(); index++) {
            for (Property property : edgeInfo.getPropertyGroupByIndex(index)) {
                expectedProperties.add(property.getName());
            }
        }
        for (EdgeRecord record : records) {
            if (record == null || record.source() < 0 || record.destination() < 0) {
                throw new IllegalArgumentException("Topology IDs must be non-negative.");
            }
            if (aligned(record, layout) >= alignedVertexCount) {
                throw new IllegalArgumentException(
                        "Aligned edge endpoint exceeds declared vertex count.");
            }
            if (!record.properties().keySet().equals(expectedProperties)) {
                throw new IllegalArgumentException(
                        "Edge record properties do not match EdgeInfo property groups.");
            }
        }
    }

    private static void validatePropertyCardinality(PropertyGroup propertyGroup, Row row) {
        int index = 0;
        for (Property property : propertyGroup) {
            Object value = row.value(index++);
            if (value == null) {
                continue;
            }
            if (property.getCardinality() != Cardinality.SINGLE
                    || property.getDataType().isList()) {
                if (!(value instanceof List<?>)) {
                    throw new IllegalArgumentException(
                            "GraphAr list property must be represented by a List: "
                                    + property.getName());
                }
                if (property.getCardinality() == Cardinality.SET) {
                    List<?> values = (List<?>) value;
                    if (new HashSet<>(values).size() != values.size()) {
                        throw new IllegalArgumentException(
                                "GraphAr set property contains duplicate values: "
                                        + property.getName());
                    }
                }
            }
        }
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

    private static Row vertexRow(long vertexId, Row source, Schema sourceSchema) {
        Object[] values = new Object[sourceSchema.fields().size() + 1];
        values[0] = vertexId;
        for (int index = 0; index < sourceSchema.fields().size(); index++) {
            values[index + 1] = source.value(index);
        }
        return new ArrayRow(values);
    }

    private final class PartitionEdgeWriter {
        private final EdgeInfo edgeInfo;
        private final AdjListType layout;
        private final long partition;
        private final long partitionEnd;
        private final List<PropertyGroup> propertyGroups;
        private final EdgeRecordCodec codec;
        private final Path workDirectory;
        private final Path offsetPath;
        private DataOutputStream offsetOutput;
        private long offsetCount;
        private long nextOffsetVertex;
        private long edgeCount;
        private long edgeChunk;
        private long rowsInChunk;
        private Path chunkPath;
        private DataOutputStream chunkOutput;

        private PartitionEdgeWriter(
                EdgeInfo edgeInfo,
                AdjListType layout,
                long partition,
                long partitionStart,
                long partitionEnd,
                List<PropertyGroup> propertyGroups,
                EdgeRecordCodec codec,
                Path workDirectory)
                throws IOException {
            this.edgeInfo = edgeInfo;
            this.layout = layout;
            this.partition = partition;
            this.partitionEnd = partitionEnd;
            this.propertyGroups = propertyGroups;
            this.codec = codec;
            this.workDirectory = workDirectory;
            this.offsetPath =
                    layout.isOrdered()
                            ? workDirectory.resolve("offset-" + partition + ".bin")
                            : null;
            if (offsetPath != null) {
                this.offsetOutput = output(offsetPath);
                appendOffset(0);
            }
            this.nextOffsetVertex = partitionStart;
        }

        private void accept(EdgeRecord record) throws IOException {
            long aligned = aligned(record, layout);
            if (aligned < nextOffsetVertex || aligned >= partitionEnd) {
                throw new IllegalStateException("External edge partition order is invalid.");
            }
            if (offsetOutput != null) {
                while (nextOffsetVertex < aligned) {
                    appendOffset(edgeCount);
                    nextOffsetVertex++;
                }
            }
            if (chunkOutput == null) {
                chunkPath = workDirectory.resolve("output-" + partition + "-" + edgeChunk + ".bin");
                chunkOutput = output(chunkPath);
            }
            codec.write(chunkOutput, record);
            rowsInChunk++;
            edgeCount = Math.addExact(edgeCount, 1);
            if (rowsInChunk == edgeInfo.getChunkSize()) {
                flushChunk();
            }
        }

        private long finish() throws IOException {
            flushChunk();
            if (offsetOutput != null) {
                while (nextOffsetVertex < partitionEnd) {
                    appendOffset(edgeCount);
                    nextOffsetVertex++;
                }
                offsetOutput.close();
                offsetOutput = null;
                try {
                    physicalWriter.write(
                            new WriteRequest(
                                    absolute(edgeInfo.getOffsetChunkUri(layout, partition)),
                                    OFFSET_SCHEMA,
                                    writeMode),
                            new LongFileBatchCursor(offsetPath, offsetCount));
                } finally {
                    delete(offsetPath);
                }
            }
            writeLong(edgeInfo.getEdgesNumFileUri(layout, partition), edgeCount);
            return edgeCount;
        }

        private void flushChunk() throws IOException {
            if (chunkOutput == null) {
                return;
            }
            chunkOutput.close();
            try {
                writeEdgeChunkFromSpill(
                        edgeInfo,
                        layout,
                        partition,
                        edgeChunk++,
                        chunkPath,
                        rowsInChunk,
                        codec,
                        propertyGroups);
            } finally {
                delete(chunkPath);
                chunkPath = null;
                chunkOutput = null;
                rowsInChunk = 0;
            }
        }

        private void appendOffset(long value) throws IOException {
            offsetOutput.writeLong(value);
            offsetCount++;
        }
    }

    private static final class EdgeRecordCodec {
        private final List<Property> properties;
        private final Set<String> propertyNames;

        private EdgeRecordCodec(List<Property> properties) {
            this.properties = properties;
            Set<String> names = new HashSet<>();
            for (Property property : properties) {
                names.add(property.getName());
            }
            this.propertyNames = Set.copyOf(names);
        }

        private void write(DataOutputStream output, EdgeRecord record) throws IOException {
            output.writeLong(record.source());
            output.writeLong(record.destination());
            for (Property property : properties) {
                Object value = record.properties().get(property.getName());
                output.writeBoolean(value != null);
                if (value != null) {
                    writeValue(output, property.getDataType(), value);
                }
            }
        }

        private EdgeRecord read(DataInputStream input) throws IOException {
            long source = input.readLong();
            long destination = input.readLong();
            Map<String, Object> values = new LinkedHashMap<>();
            for (Property property : properties) {
                values.put(
                        property.getName(),
                        input.readBoolean() ? readValue(input, property.getDataType()) : null);
            }
            return new EdgeRecord(source, destination, values);
        }

        private void writeValue(DataOutputStream output, DataType type, Object value)
                throws IOException {
            if (type.isList()) {
                if (!(value instanceof List<?>)) {
                    throw new IllegalArgumentException(
                            "GraphAr list property must be represented by a List.");
                }
                List<?> values = (List<?>) value;
                output.writeInt(values.size());
                for (Object element : values) {
                    if (element == null) {
                        throw new IllegalArgumentException(
                                "GraphAr list properties cannot contain null values.");
                    }
                    writeValue(output, type.getValueType(), element);
                }
                return;
            }
            if (type.equals(DataType.BOOL)) {
                output.writeBoolean((Boolean) value);
            } else if (type.equals(DataType.INT32)) {
                output.writeInt(((Number) value).intValue());
            } else if (type.equals(DataType.INT64)) {
                output.writeLong(((Number) value).longValue());
            } else if (type.equals(DataType.FLOAT)) {
                output.writeFloat(((Number) value).floatValue());
            } else if (type.equals(DataType.DOUBLE)) {
                output.writeDouble(((Number) value).doubleValue());
            } else if (type.equals(DataType.STRING)) {
                byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                output.writeInt(bytes.length);
                output.write(bytes);
            } else if (type.equals(DataType.DATE)) {
                output.writeLong(((LocalDate) value).toEpochDay());
            } else if (type.equals(DataType.TIMESTAMP)) {
                output.writeLong(((Instant) value).toEpochMilli());
            } else {
                throw new IllegalArgumentException("Unsupported GraphAr property type: " + type);
            }
        }

        private Object readValue(DataInputStream input, DataType type) throws IOException {
            if (type.isList()) {
                int size = input.readInt();
                if (size < 0) {
                    throw new IOException("Invalid negative GraphAr list length in spill file.");
                }
                List<Object> values = new ArrayList<>(size);
                for (int index = 0; index < size; index++) {
                    values.add(readValue(input, type.getValueType()));
                }
                return values;
            }
            if (type.equals(DataType.BOOL)) return input.readBoolean();
            if (type.equals(DataType.INT32)) return input.readInt();
            if (type.equals(DataType.INT64)) return input.readLong();
            if (type.equals(DataType.FLOAT)) return input.readFloat();
            if (type.equals(DataType.DOUBLE)) return input.readDouble();
            if (type.equals(DataType.STRING)) {
                int size = input.readInt();
                if (size < 0)
                    throw new IOException("Invalid negative string length in spill file.");
                byte[] bytes = new byte[size];
                input.readFully(bytes);
                return new String(bytes, StandardCharsets.UTF_8);
            }
            if (type.equals(DataType.DATE)) return LocalDate.ofEpochDay(input.readLong());
            if (type.equals(DataType.TIMESTAMP)) return Instant.ofEpochMilli(input.readLong());
            throw new IOException("Unsupported GraphAr property type: " + type);
        }
    }

    private static final class PartitionSpillWriter implements AutoCloseable {
        private static final int MAX_OPEN_PARTITIONS = 32;

        private final Path workDirectory;
        private final EdgeRecordCodec codec;
        private final LinkedHashMap<Long, DataOutputStream> outputs =
                new LinkedHashMap<>(16, 0.75F, true);

        private PartitionSpillWriter(Path workDirectory, EdgeRecordCodec codec) {
            this.workDirectory = workDirectory;
            this.codec = codec;
        }

        private void write(long partition, EdgeRecord record) throws IOException {
            DataOutputStream output = outputs.get(partition);
            if (output == null) {
                if (outputs.size() == MAX_OPEN_PARTITIONS) {
                    Iterator<Map.Entry<Long, DataOutputStream>> iterator =
                            outputs.entrySet().iterator();
                    Map.Entry<Long, DataOutputStream> eldest = iterator.next();
                    eldest.getValue().close();
                    iterator.remove();
                }
                output =
                        new DataOutputStream(
                                new BufferedOutputStream(
                                        Files.newOutputStream(
                                                partitionPath(workDirectory, partition),
                                                StandardOpenOption.CREATE,
                                                StandardOpenOption.APPEND,
                                                StandardOpenOption.WRITE)));
                outputs.put(partition, output);
            }
            codec.write(output, record);
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            for (DataOutputStream output : outputs.values()) {
                try {
                    output.close();
                } catch (IOException exception) {
                    if (failure == null) failure = exception;
                    else failure.addSuppressed(exception);
                }
            }
            outputs.clear();
            if (failure != null) throw failure;
        }
    }

    private static final class EdgeRecordInput implements AutoCloseable {
        private final DataInputStream input;
        private final EdgeRecordCodec codec;

        private EdgeRecordInput(Path path, EdgeRecordCodec codec) throws IOException {
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
            this.codec = codec;
        }

        private EdgeRecord next() throws IOException {
            try {
                return codec.read(input);
            } catch (EOFException end) {
                return null;
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static final class EdgeFileBatchCursor implements BatchCursor {
        private static final int BATCH_ROWS = 1024;

        private final EdgeRecordInput input;
        private final long count;
        private final Schema schema;
        private final EdgeRowMapper mapper;
        private long read;
        private RecordBatch batch;

        private EdgeFileBatchCursor(
                Path path, long count, EdgeRecordCodec codec, Schema schema, EdgeRowMapper mapper)
                throws IOException {
            this.input = new EdgeRecordInput(path, codec);
            this.count = count;
            this.schema = schema;
            this.mapper = mapper;
        }

        @Override
        public boolean next() throws IOException {
            if (read == count) {
                return false;
            }
            int batchSize = Math.toIntExact(Math.min(BATCH_ROWS, count - read));
            List<Row> rows = new ArrayList<>(batchSize);
            for (int index = 0; index < batchSize; index++) {
                EdgeRecord record = input.next();
                if (record == null) {
                    throw new IOException("Unexpected end of GraphAr edge spill chunk.");
                }
                rows.add(mapper.map(record));
            }
            read += batchSize;
            batch = new ListRecordBatch(schema, rows);
            return true;
        }

        @Override
        public RecordBatch batch() {
            if (batch == null) throw new IllegalStateException("Call next() before batch().");
            return batch;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static final class LongFileBatchCursor implements BatchCursor {
        private static final int BATCH_ROWS = 1024;

        private final DataInputStream input;
        private final long count;
        private long read;
        private RecordBatch batch;

        private LongFileBatchCursor(Path path, long count) throws IOException {
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
            this.count = count;
        }

        @Override
        public boolean next() throws IOException {
            if (read == count) {
                return false;
            }
            int batchSize = Math.toIntExact(Math.min(BATCH_ROWS, count - read));
            List<Row> rows = new ArrayList<>(batchSize);
            for (int index = 0; index < batchSize; index++) {
                rows.add(new ArrayRow(new Object[] {input.readLong()}));
            }
            read += batchSize;
            batch = new ListRecordBatch(OFFSET_SCHEMA, rows);
            return true;
        }

        @Override
        public RecordBatch batch() {
            if (batch == null) throw new IllegalStateException("Call next() before batch().");
            return batch;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    @FunctionalInterface
    private interface EdgeConsumer {
        void accept(EdgeRecord record) throws IOException;
    }

    private interface EdgeRowMapper {
        Row map(EdgeRecord record);
    }

    private static final class RunHead {
        private final int run;
        private final EdgeRecord record;

        private RunHead(int run, EdgeRecord record) {
            this.run = run;
            this.record = record;
        }
    }

    private static final class RunSet {
        private final List<Path> paths;
        private final int peakRecords;

        private RunSet(List<Path> paths, int peakRecords) {
            this.paths = paths;
            this.peakRecords = peakRecords;
        }
    }

    private static final class PartitionOutcome {
        private final long edgeCount;
        private final long spillRuns;
        private final int peakRecords;

        private PartitionOutcome(long edgeCount, long spillRuns, int peakRecords) {
            this.edgeCount = edgeCount;
            this.spillRuns = spillRuns;
            this.peakRecords = peakRecords;
        }
    }

    private static final class MergeSet {
        private final List<Path> paths;
        private final long createdRuns;

        private MergeSet(List<Path> paths, long createdRuns) {
            this.paths = paths;
            this.createdRuns = createdRuns;
        }
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
