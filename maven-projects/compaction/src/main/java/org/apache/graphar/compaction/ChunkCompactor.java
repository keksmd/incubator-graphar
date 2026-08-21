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

package org.apache.graphar.compaction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.graphar.core.ChunkMath;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.reader.EdgeLayoutReader;
import org.apache.graphar.reader.EdgePropertyCursor;
import org.apache.graphar.reader.GraphEdge;
import org.apache.graphar.reader.GraphReader;
import org.apache.graphar.storage.Storage;
import org.apache.graphar.writer.EdgeRecord;
import org.apache.graphar.writer.EdgeWriteOptions;
import org.apache.graphar.writer.EdgeWriteStats;
import org.apache.graphar.writer.GraphWriter;

/**
 * Folds the patches accumulated for one chunk back into that chunk of the base dataset.
 *
 * <p>A dataset that absorbs changes as patches answers a read as base plus patches, which gets more
 * expensive the more patches it holds. Recomputing the whole dataset makes them cheap again but
 * costs a rewrite of everything, including the parts nothing changed in. GraphAr stores offsets,
 * the adjacency chunk sequence, and the edge count of a vertex-aligned partition inside that
 * partition, which is what allows the cheaper trade this class implements: rewrite one chunk from
 * its own rows plus the patches addressed to it, and drop exactly those patches, leaving every
 * other chunk of the base byte-identical.
 *
 * <p>The rewritten chunk equals what a rebuild of that partition over the base rows followed by the
 * patches produces, which is the same order the patches were accumulated in. That holds because the
 * writer sorts stably by aligned endpoint, so rows that share one endpoint keep the order they were
 * fed in.
 *
 * <p>A compaction is not one observable step: while it runs, the dataset holds part of the old
 * partition next to part of the new one. Run it inside {@link
 * org.apache.graphar.reader.GraphProjection#rewriteDataset}, which keeps a rebuild from reading
 * that mixture, and publish the result afterwards.
 *
 * <p>The patches are dropped after the rewritten partition has been published, so a failure between
 * the two leaves them pending and folds them in again on the next attempt. This makes compaction
 * at-least-once rather than exactly-once; the alternative would be dropping patches that a failed
 * rewrite never stored.
 */
public final class ChunkCompactor {
    private final GraphReader reader;
    private final GraphWriter writer;
    private final Storage storage;
    private final EdgeWriteOptions options;

    /** Creates a compactor that rewrites chunks with the default write options. */
    public ChunkCompactor(GraphReader reader, GraphWriter writer, Storage storage) {
        this(reader, writer, storage, EdgeWriteOptions.defaults());
    }

    /**
     * Creates a compactor over one dataset. The writer must be in {@link
     * org.apache.graphar.io.WriteMode#OVERWRITE} and must target the dataset the reader reads,
     * because a compaction replaces outputs that already exist.
     */
    public ChunkCompactor(
            GraphReader reader, GraphWriter writer, Storage storage, EdgeWriteOptions options) {
        this.reader = Objects.requireNonNull(reader, "Graph reader cannot be null.");
        this.writer = Objects.requireNonNull(writer, "Graph writer cannot be null.");
        this.storage = Objects.requireNonNull(storage, "Storage cannot be null.");
        this.options = Objects.requireNonNull(options, "Edge write options cannot be null.");
    }

    /**
     * Rewrites one chunk from its base rows plus everything {@code patches} holds for it, drops
     * exactly that slice, and reports what the rewrite cost.
     *
     * <p>A chunk with nothing pending is left untouched and reported as a rewrite of no rows, so a
     * caller driving compaction by measured cost can ask for a chunk without checking first.
     *
     * @throws IllegalStateException when the writer is not in overwrite mode
     * @throws IllegalArgumentException when the chunk is outside the layout, or a patch is
     *     addressed to a different chunk than the one being compacted
     */
    public CompactedChunk compact(EdgeChunk chunk, ChunkPatchSource patches) throws IOException {
        Objects.requireNonNull(chunk, "Edge chunk cannot be null.");
        Objects.requireNonNull(patches, "Patch source cannot be null.");
        EdgeInfo edgeInfo =
                reader.graphInfo().getEdgeInfo(chunk.srcType(), chunk.edgeType(), chunk.dstType());
        EdgeLayoutReader layoutReader =
                reader.edge(chunk.srcType(), chunk.edgeType(), chunk.dstType(), chunk.layout());
        long alignedVertexCount = layoutReader.vertexCount();
        long baseEdgeCount = layoutReader.partitionEdgeCount(chunk.partition());
        ChunkPatches pending = patches.patchesFor(chunk);
        if (!pending.chunk().equals(chunk)) {
            throw new IllegalArgumentException(
                    "Patch source returned patches for "
                            + pending.chunk()
                            + " and not for "
                            + chunk);
        }
        if (pending.isEmpty()) {
            return new CompactedChunk(
                    chunk,
                    pending.watermark(),
                    baseEdgeCount,
                    0L,
                    baseEdgeCount,
                    ChunkMath.chunkCount(baseEdgeCount, edgeInfo.getChunkSize()),
                    0L);
        }
        EdgeWriteStats stats;
        try (EdgePropertyCursor base = layoutReader.scanPartition(chunk.partition())) {
            stats =
                    writer.writeEdgePartition(
                            edgeInfo,
                            chunk.layout(),
                            alignedVertexCount,
                            chunk.partition(),
                            () -> new FoldedRecords(base, pending.records()),
                            options);
        } catch (UncheckedIOException failure) {
            throw failure.getCause();
        }
        long adjacencyChunks = ChunkMath.chunkCount(stats.edgeCount(), edgeInfo.getChunkSize());
        CompactedChunk compacted =
                new CompactedChunk(
                        chunk,
                        pending.watermark(),
                        baseEdgeCount,
                        pending.records().size(),
                        stats.edgeCount(),
                        adjacencyChunks,
                        measure(edgeInfo, chunk, adjacencyChunks));
        patches.drop(compacted);
        return compacted;
    }

    private long measure(EdgeInfo edgeInfo, EdgeChunk chunk, long adjacencyChunks)
            throws IOException {
        long total = 0L;
        for (long index = 0; index < adjacencyChunks; index++) {
            total +=
                    sizeOf(
                            edgeInfo.getAdjacentListChunkUri(
                                    chunk.layout(), chunk.partition(), index));
            for (PropertyGroup group : edgeInfo.getPropertyGroups()) {
                total +=
                        sizeOf(
                                edgeInfo.getPropertyGroupChunkUri(
                                        group, chunk.layout(), chunk.partition(), index));
            }
        }
        if (chunk.layout().isOrdered()) {
            total += sizeOf(edgeInfo.getOffsetChunkUri(chunk.layout(), chunk.partition()));
        }
        total += sizeOf(edgeInfo.getEdgesNumFileUri(chunk.layout(), chunk.partition()));
        return total;
    }

    private long sizeOf(URI uri) throws IOException {
        URI resolved = uri.isAbsolute() ? uri : writer.datasetRoot().resolve(uri);
        return storage.exists(resolved) ? storage.inputFile(resolved).size() : 0L;
    }

    /**
     * Presents the base rows of a partition followed by its patches as one sequence of records.
     *
     * <p>The base rows come first and in physical order, so the rewrite starts from exactly what a
     * rebuild of the partition would have produced up to now and then applies what arrived after
     * it.
     */
    private static final class FoldedRecords implements Iterator<EdgeRecord> {
        private final EdgePropertyCursor base;
        private final Iterator<EdgeRecord> patches;
        private boolean baseDrained;
        private boolean baseReady;

        private FoldedRecords(EdgePropertyCursor base, List<EdgeRecord> patches) {
            this.base = base;
            this.patches = patches.iterator();
        }

        @Override
        public boolean hasNext() {
            return advanceBase() || patches.hasNext();
        }

        @Override
        public EdgeRecord next() {
            if (advanceBase()) {
                baseReady = false;
                GraphEdge edge = base.edge();
                return new EdgeRecord(edge.source(), edge.destination(), edge.properties());
            }
            if (!patches.hasNext()) {
                throw new NoSuchElementException("No edge record left to fold.");
            }
            return patches.next();
        }

        private boolean advanceBase() {
            if (baseReady) {
                return true;
            }
            if (baseDrained) {
                return false;
            }
            try {
                baseReady = base.next();
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
            baseDrained = !baseReady;
            return baseReady;
        }
    }
}
