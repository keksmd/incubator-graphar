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

package org.apache.graphar.compaction.cost;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.graphar.core.ChunkRange;

/**
 * Answers what compacting a chunk would rewrite and what it would reclaim, without compacting it.
 *
 * <p>It joins the two halves of the question that live apart: the Base side, whose cost is the
 * measured size of the chunks a rewrite has to write again, and the Delta side, whose gain is what
 * the ledger holds against those chunks. Nothing here reads or writes data - an estimate is
 * arithmetic over sizes already known, which is what makes it usable per chunk and often.
 *
 * <p>The per-entry byte cost used to price patched entries is taken from the chunk they land in,
 * because that chunk is the only honest sample of what an entry costs after compression in this
 * dataset. A chunk holding no entries of its own is priced at the dataset-wide average, and a chunk
 * past the end of Base - an append-tail chunk not yet published - costs nothing to carry over and
 * is priced the same way.
 *
 * <p>{@link #retainedInDelta(double)} and {@link #compactionCandidates(double)} split the chunks
 * the way the retention policy asks for: the worst-paying rewrites are named as the set to leave in
 * the Delta, and the split point is a share of the measured rewrite total, not a constant.
 */
public final class CompactionEstimator {
    private final BaseChunkLayout layout;
    private final DeltaCostSnapshot delta;
    private final double datasetBytesPerEntry;

    /** Joins a measured Base layout with a Delta snapshot taken over the same projection. */
    public CompactionEstimator(BaseChunkLayout layout, DeltaCostSnapshot delta) {
        this.layout = Objects.requireNonNull(layout, "Layout cannot be null.");
        this.delta = Objects.requireNonNull(delta, "Delta snapshot cannot be null.");
        if (layout.vertexChunkSize() != delta.vertexChunkSize()) {
            throw new IllegalArgumentException(
                    "Layout and Delta disagree on the vertex chunk size: "
                            + layout.vertexChunkSize()
                            + " against "
                            + delta.vertexChunkSize());
        }
        long bytes = 0;
        long entries = 0;
        for (long chunkIndex = 0; chunkIndex < layout.chunkCount(); chunkIndex++) {
            bytes += layout.baseBytes(chunkIndex);
            entries += layout.baseEdges(chunkIndex);
        }
        this.datasetBytesPerEntry = entries == 0 ? 0.0d : bytes / (double) entries;
    }

    /** Returns the Base layout the estimates are priced against. */
    public BaseChunkLayout layout() {
        return layout;
    }

    /** Returns the Delta snapshot the estimates are taken over. */
    public DeltaCostSnapshot delta() {
        return delta;
    }

    /** Estimates compacting one chunk. */
    public CompactionEstimate estimate(long chunkIndex) {
        return estimate(new long[] {chunkIndex});
    }

    /** Estimates compacting a contiguous range of chunks in one pass. */
    public CompactionEstimate estimate(ChunkRange range) {
        Objects.requireNonNull(range, "Chunk range cannot be null.");
        long[] indexes = new long[Math.toIntExact(range.end() - range.begin())];
        for (int position = 0; position < indexes.length; position++) {
            indexes[position] = range.begin() + position;
        }
        return estimate(indexes);
    }

    /**
     * Estimates compacting a set of chunks. Duplicates collapse, and a chunk with no patches keeps
     * counting towards the rewrite cost, because rewriting it still writes its bytes again.
     */
    public CompactionEstimate estimate(long[] chunkIndexes) {
        Objects.requireNonNull(chunkIndexes, "Chunk indexes cannot be null.");
        long[] distinct = distinctSorted(chunkIndexes);
        long affectedChunks = 0;
        long affectedVertices = 0;
        long baseBytes = 0;
        long baseEdges = 0;
        long deltaEntries = 0;
        long deltaBytes = 0;
        long rewriteBytes = 0;
        for (long chunkIndex : distinct) {
            long chunkBaseBytes = baseBytesOf(chunkIndex);
            long chunkBaseEdges = baseEdgesOf(chunkIndex);
            ChunkDelta patches = delta.chunk(chunkIndex);
            if (!patches.isEmpty()) {
                affectedChunks++;
            }
            affectedVertices += patches.patchedVertices();
            baseBytes += chunkBaseBytes;
            baseEdges += chunkBaseEdges;
            deltaEntries += patches.entries();
            deltaBytes += patches.bytes();
            rewriteBytes +=
                    chunkBaseBytes + Math.round(patches.entries() * bytesPerEntry(chunkIndex));
        }
        return new CompactionEstimate(
                distinct,
                affectedChunks,
                affectedVertices,
                baseBytes,
                baseEdges,
                deltaEntries,
                deltaBytes,
                rewriteBytes);
    }

    /**
     * Estimates folding the whole Delta into Base, which is the rewrite of exactly the chunks it
     * patches.
     */
    public CompactionEstimate estimateAll() {
        return estimate(delta.chunkIndexes());
    }

    /** Returns one estimate per patched chunk, the most rewarding rewrite first. */
    public List<CompactionEstimate> perChunk() {
        List<CompactionEstimate> estimates = new ArrayList<>();
        for (long chunkIndex : delta.chunkIndexes()) {
            estimates.add(estimate(chunkIndex));
        }
        estimates.sort(
                Comparator.comparingDouble(CompactionEstimate::compactionUtility)
                        .reversed()
                        .thenComparingLong(estimate -> estimate.chunkIndexes()[0]));
        return Collections.unmodifiableList(estimates);
    }

    /**
     * Returns how concentrated the potential rewrite is over the patched chunks: the reading behind
     * any claim that a few chunks account for most of the work.
     */
    public ConcentrationProfile rewriteConcentration() {
        long[] chunkIndexes = delta.chunkIndexes();
        long[] rewriteBytes = new long[chunkIndexes.length];
        for (int position = 0; position < chunkIndexes.length; position++) {
            rewriteBytes[position] = estimate(chunkIndexes[position]).estimatedRewriteBytes();
        }
        return ConcentrationProfile.of(rewriteBytes);
    }

    /**
     * Returns the chunks to leave in the Delta: the worst-paying rewrites, taken until they account
     * for {@code retainedRewriteShare} of the rewrite the whole Delta would cost.
     *
     * <p>This is the retention rule stated as arithmetic. Whether the share the caller passes buys
     * back a small set of chunks or nearly all of them is a property of the workload, readable in
     * advance from {@link #rewriteConcentration()}.
     */
    public List<CompactionEstimate> retainedInDelta(double retainedRewriteShare) {
        return split(retainedRewriteShare, true);
    }

    /**
     * Returns the chunks worth compacting under the same split, the most rewarding first: every
     * patched chunk that {@link #retainedInDelta(double)} did not hold back.
     */
    public List<CompactionEstimate> compactionCandidates(double retainedRewriteShare) {
        return split(retainedRewriteShare, false);
    }

    /** Estimates {@code read_penalty(delta)} against a read distribution. */
    public ReadPenalty readPenalty(ReadProfile profile) {
        Objects.requireNonNull(profile, "Read profile cannot be null.");
        long[] chunkIndexes = readableChunks();
        double totalWeight = 0.0d;
        double patchedShare = 0.0d;
        double extraEntries = 0.0d;
        double extraBytes = 0.0d;
        double baseEntries = 0.0d;
        for (long chunkIndex : chunkIndexes) {
            double weight = profile.reads(chunkIndex);
            if (weight < 0) {
                throw new IllegalArgumentException(
                        "Read weights must be non-negative: "
                                + weight
                                + " for chunk "
                                + chunkIndex);
            }
            if (weight == 0.0d) {
                continue;
            }
            double vertices = verticesOf(chunkIndex);
            ChunkDelta patches = delta.chunk(chunkIndex);
            totalWeight += weight;
            patchedShare += weight * (patches.patchedVertices() / vertices);
            extraEntries += weight * (patches.entries() / vertices);
            extraBytes += weight * (patches.bytes() / vertices);
            baseEntries += weight * (baseEdgesOf(chunkIndex) / vertices);
        }
        if (totalWeight == 0.0d) {
            return new ReadPenalty(0.0d, 0.0d, 0.0d, 0.0d);
        }
        return new ReadPenalty(
                patchedShare / totalWeight,
                extraEntries / totalWeight,
                extraBytes / totalWeight,
                baseEntries / totalWeight);
    }

    private List<CompactionEstimate> split(double retainedRewriteShare, boolean retained) {
        if (!(retainedRewriteShare >= 0.0d) || retainedRewriteShare > 1.0d) {
            throw new IllegalArgumentException(
                    "Retained rewrite share must be within [0, 1]: " + retainedRewriteShare);
        }
        List<CompactionEstimate> byUtility = perChunk();
        long total = 0;
        for (CompactionEstimate estimate : byUtility) {
            total += estimate.estimatedRewriteBytes();
        }
        double budget = retainedRewriteShare * total;
        double accumulated = 0.0d;
        int heldCount = 0;
        for (int position = byUtility.size() - 1;
                position >= 0 && accumulated < budget;
                position--) {
            accumulated += byUtility.get(position).estimatedRewriteBytes();
            heldCount++;
        }
        int firstHeld = byUtility.size() - heldCount;
        if (!retained) {
            return Collections.unmodifiableList(new ArrayList<>(byUtility.subList(0, firstHeld)));
        }
        List<CompactionEstimate> held = new ArrayList<>(heldCount);
        for (int position = byUtility.size() - 1; position >= firstHeld; position--) {
            held.add(byUtility.get(position));
        }
        return Collections.unmodifiableList(held);
    }

    private long[] readableChunks() {
        long[] deltaChunks = delta.chunkIndexes();
        long baseChunks = layout.chunkCount();
        List<Long> union = new ArrayList<>();
        for (long chunkIndex = 0; chunkIndex < baseChunks; chunkIndex++) {
            union.add(chunkIndex);
        }
        for (long chunkIndex : deltaChunks) {
            if (chunkIndex >= baseChunks) {
                union.add(chunkIndex);
            }
        }
        long[] chunkIndexes = new long[union.size()];
        for (int position = 0; position < chunkIndexes.length; position++) {
            chunkIndexes[position] = union.get(position);
        }
        return chunkIndexes;
    }

    private double verticesOf(long chunkIndex) {
        return chunkIndex < layout.chunkCount()
                ? layout.verticesInChunk(chunkIndex)
                : layout.vertexChunkSize();
    }

    private long baseBytesOf(long chunkIndex) {
        return chunkIndex < layout.chunkCount() ? layout.baseBytes(chunkIndex) : 0L;
    }

    private long baseEdgesOf(long chunkIndex) {
        return chunkIndex < layout.chunkCount() ? layout.baseEdges(chunkIndex) : 0L;
    }

    private double bytesPerEntry(long chunkIndex) {
        long entries = baseEdgesOf(chunkIndex);
        return entries == 0 ? datasetBytesPerEntry : baseBytesOf(chunkIndex) / (double) entries;
    }

    private static long[] distinctSorted(long[] chunkIndexes) {
        long[] sorted = chunkIndexes.clone();
        Arrays.sort(sorted);
        int size = 0;
        for (int position = 0; position < sorted.length; position++) {
            if (sorted[position] < 0) {
                throw new IllegalArgumentException(
                        "Chunk must be non-negative: " + sorted[position]);
            }
            if (position == 0 || sorted[position] != sorted[position - 1]) {
                sorted[size++] = sorted[position];
            }
        }
        return Arrays.copyOf(sorted, size);
    }
}
