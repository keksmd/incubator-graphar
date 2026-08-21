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

/**
 * What compacting a chunk, or a set of chunks, would cost and what it would give back.
 *
 * <p>Every number here is an estimate produced without touching the data: the Base side comes from
 * the measured size of the chunks a rewrite would have to write again, the Delta side from what the
 * ledger holds against those chunks. The two ratios are the ones the decision is stated in - {@link
 * #compactionAmplification()} is the price, {@link #compactionUtility()} is the return - and
 * ranking candidates by the second while bounding the first is what replaces a compaction timer.
 */
public final class CompactionEstimate {
    private final long[] chunkIndexes;
    private final long affectedChunks;
    private final long affectedVertices;
    private final long baseBytesTouched;
    private final long baseEdgesTouched;
    private final long deltaEntriesRemovedIfCompacted;
    private final long deltaBytesRemovedIfCompacted;
    private final long estimatedRewriteBytes;

    CompactionEstimate(
            long[] chunkIndexes,
            long affectedChunks,
            long affectedVertices,
            long baseBytesTouched,
            long baseEdgesTouched,
            long deltaEntriesRemovedIfCompacted,
            long deltaBytesRemovedIfCompacted,
            long estimatedRewriteBytes) {
        this.chunkIndexes = chunkIndexes;
        this.affectedChunks = affectedChunks;
        this.affectedVertices = affectedVertices;
        this.baseBytesTouched = baseBytesTouched;
        this.baseEdgesTouched = baseEdgesTouched;
        this.deltaEntriesRemovedIfCompacted = deltaEntriesRemovedIfCompacted;
        this.deltaBytesRemovedIfCompacted = deltaBytesRemovedIfCompacted;
        this.estimatedRewriteBytes = estimatedRewriteBytes;
    }

    /** Returns the chunks the rewrite would produce again, ascending. */
    public long[] chunkIndexes() {
        return chunkIndexes.clone();
    }

    /** Returns the chunk this estimate is about, for a single-chunk estimate. */
    public long chunkIndex() {
        if (chunkIndexes.length != 1) {
            throw new IllegalStateException(
                    "Estimate covers " + chunkIndexes.length + " chunks, not one.");
        }
        return chunkIndexes[0];
    }

    /** Returns how many chunks the rewrite would produce again. */
    public long rewrittenChunks() {
        return chunkIndexes.length;
    }

    /**
     * Returns {@code affected_chunks}: how many of the rewritten chunks actually carry Delta. A
     * rewritten chunk that carries none is pure cost, and keeping the two counts apart is what
     * makes that visible.
     */
    public long affectedChunks() {
        return affectedChunks;
    }

    /** Returns {@code affected_vertices}: distinct patched vertices inside the rewritten chunks. */
    public long affectedVertices() {
        return affectedVertices;
    }

    /** Returns {@code base_bytes_touched}: stored Base bytes the rewrite has to write again. */
    public long baseBytesTouched() {
        return baseBytesTouched;
    }

    /** Returns the Base adjacency entries inside the rewritten chunks. */
    public long baseEdgesTouched() {
        return baseEdgesTouched;
    }

    /** Returns {@code delta_bytes_removed_if_compacted}. */
    public long deltaBytesRemovedIfCompacted() {
        return deltaBytesRemovedIfCompacted;
    }

    /** Returns the Delta adjacency entries the rewrite would fold into Base. */
    public long deltaEntriesRemovedIfCompacted() {
        return deltaEntriesRemovedIfCompacted;
    }

    /**
     * Returns {@code estimated_rewrite_bytes}: the Base bytes carried over plus the patched entries
     * encoded at the per-entry cost measured on those same chunks.
     */
    public long estimatedRewriteBytes() {
        return estimatedRewriteBytes;
    }

    /**
     * Returns {@code compaction_amplification = base_bytes_rewritten / delta_bytes_eliminated}: how
     * many Base bytes are paid per Delta byte removed. Lower is better, and a rewrite that would
     * remove nothing returns {@link Double#POSITIVE_INFINITY} rather than a finite price for
     * nothing.
     */
    public double compactionAmplification() {
        if (deltaBytesRemovedIfCompacted == 0) {
            return baseBytesTouched == 0 ? 0.0d : Double.POSITIVE_INFINITY;
        }
        return baseBytesTouched / (double) deltaBytesRemovedIfCompacted;
    }

    /**
     * Returns {@code compaction_utility = delta_reclaimed / rewrite_cost}: Delta bytes removed per
     * byte the rewrite writes. Higher is better, and it is the order compaction candidates are
     * ranked in.
     */
    public double compactionUtility() {
        if (estimatedRewriteBytes == 0) {
            return 0.0d;
        }
        return deltaBytesRemovedIfCompacted / (double) estimatedRewriteBytes;
    }

    @Override
    public String toString() {
        return "CompactionEstimate{chunks="
                + chunkIndexes.length
                + ", affectedChunks="
                + affectedChunks
                + ", affectedVertices="
                + affectedVertices
                + ", baseBytesTouched="
                + baseBytesTouched
                + ", deltaBytesRemoved="
                + deltaBytesRemovedIfCompacted
                + ", rewriteBytes="
                + estimatedRewriteBytes
                + ", amplification="
                + compactionAmplification()
                + ", utility="
                + compactionUtility()
                + "}";
    }
}
