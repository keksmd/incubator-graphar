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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.graphar.core.ChunkMath;

/**
 * An immutable readout of the Delta at one instant.
 *
 * <p>A decision taken over a structure that keeps changing under it is not a decision, so the
 * ledger hands out this snapshot and every quantity below is answered from the same instant: {@code
 * storage_cost(delta)} is {@link #deltaBytes()}, {@code delta_growth_rate} is {@link
 * #growthBytesPerSecond()}, {@code affected_vertices} and {@code affected_chunks} are {@link
 * #affectedVertices()} and {@link #affectedChunks()}. The chunk-local ones a rewrite is judged by
 * are read through {@link CompactionEstimator}, which needs the Base side as well.
 */
public final class DeltaCostSnapshot {
    private final long vertexChunkSize;
    private final List<VertexDelta> vertices;
    private final TreeMap<Long, ChunkDelta> chunks;
    private final long deltaEntries;
    private final long deltaBytes;
    private final long admittedEntries;
    private final long admittedBytes;
    private final long reclaimedEntries;
    private final long reclaimedBytes;
    private final Instant firstArrival;
    private final Instant lastArrival;
    private final Instant takenAt;
    private final Duration observedWindow;

    DeltaCostSnapshot(
            long vertexChunkSize,
            Map<Long, long[]> patches,
            long admittedEntries,
            long admittedBytes,
            long reclaimedEntries,
            long reclaimedBytes,
            Instant firstArrival,
            Instant lastArrival,
            Instant takenAt) {
        this.vertexChunkSize = vertexChunkSize;
        this.admittedEntries = admittedEntries;
        this.admittedBytes = admittedBytes;
        this.reclaimedEntries = reclaimedEntries;
        this.reclaimedBytes = reclaimedBytes;
        this.firstArrival = firstArrival;
        this.lastArrival = lastArrival;
        this.takenAt = takenAt;
        Duration window =
                firstArrival == null ? Duration.ZERO : Duration.between(firstArrival, takenAt);
        this.observedWindow = window.isNegative() ? Duration.ZERO : window;

        List<VertexDelta> patched = new ArrayList<>(patches.size());
        TreeMap<Long, long[]> perChunk = new TreeMap<>();
        long entries = 0;
        long bytes = 0;
        for (Map.Entry<Long, long[]> patch : patches.entrySet()) {
            long vertex = patch.getKey();
            long vertexEntries = patch.getValue()[0];
            long vertexBytes = patch.getValue()[1];
            patched.add(new VertexDelta(vertex, vertexEntries, vertexBytes));
            entries += vertexEntries;
            bytes += vertexBytes;
            long[] chunk =
                    perChunk.computeIfAbsent(
                            ChunkMath.chunkIndex(vertex, vertexChunkSize), key -> new long[3]);
            chunk[0]++;
            chunk[1] += vertexEntries;
            chunk[2] += vertexBytes;
        }
        patched.sort(
                Comparator.comparingLong(VertexDelta::bytes)
                        .reversed()
                        .thenComparingLong(VertexDelta::vertex));
        this.vertices = Collections.unmodifiableList(patched);
        this.deltaEntries = entries;
        this.deltaBytes = bytes;
        TreeMap<Long, ChunkDelta> byChunk = new TreeMap<>();
        for (Map.Entry<Long, long[]> chunk : perChunk.entrySet()) {
            long[] totals = chunk.getValue();
            byChunk.put(
                    chunk.getKey(),
                    new ChunkDelta(chunk.getKey(), totals[0], totals[1], totals[2]));
        }
        this.chunks = byChunk;
    }

    /** Returns the number of vertices one chunk covers. */
    public long vertexChunkSize() {
        return vertexChunkSize;
    }

    /** Returns the instant this snapshot was taken. */
    public Instant takenAt() {
        return takenAt;
    }

    /** Returns {@code affected_vertices}: distinct vertices the Delta currently patches. */
    public long affectedVertices() {
        return vertices.size();
    }

    /** Returns {@code affected_chunks}: distinct Base chunks the Delta currently patches. */
    public long affectedChunks() {
        return chunks.size();
    }

    /** Returns the adjacency entries the Delta currently holds. */
    public long deltaEntries() {
        return deltaEntries;
    }

    /** Returns {@code storage_cost(delta)}: the bytes the Delta currently holds. */
    public long deltaBytes() {
        return deltaBytes;
    }

    /** Returns the entries ever admitted, including the ones already folded into Base. */
    public long admittedEntries() {
        return admittedEntries;
    }

    /** Returns the bytes ever admitted, including the ones already folded into Base. */
    public long admittedBytes() {
        return admittedBytes;
    }

    /** Returns the entries compaction has already taken out of the Delta. */
    public long reclaimedEntries() {
        return reclaimedEntries;
    }

    /** Returns the bytes compaction has already taken out of the Delta. */
    public long reclaimedBytes() {
        return reclaimedBytes;
    }

    /**
     * Returns the span from the first recorded patch to this snapshot. It ends at the snapshot and
     * not at the last patch, so a Delta that stopped growing shows a falling rate instead of
     * remembering the rate it had while it was busy.
     */
    public Duration observedWindow() {
        return observedWindow;
    }

    /** Returns the instant of the last recorded patch, or {@code null} when none was recorded. */
    public Instant lastArrival() {
        return lastArrival;
    }

    /** Returns the instant of the first recorded patch, or {@code null} when none was recorded. */
    public Instant firstArrival() {
        return firstArrival;
    }

    /**
     * Returns {@code delta_growth_rate} in bytes per second: everything admitted over the observed
     * window. Admission and not retention is the rate the Delta grows at, because reclaiming is the
     * decision this number is an input to.
     *
     * <p>Returns zero when nothing was admitted and {@link Double#POSITIVE_INFINITY} when bytes
     * were admitted inside a window of zero length, which a fixed clock can produce.
     */
    public double growthBytesPerSecond() {
        return perSecond(admittedBytes);
    }

    /** Returns {@code delta_growth_rate} in adjacency entries per second. */
    public double growthEntriesPerSecond() {
        return perSecond(admittedEntries);
    }

    /**
     * Returns the rate the retained Delta grows at, admissions minus reclamations over the observed
     * window. A negative value means compaction is currently outrunning ingest.
     */
    public double netGrowthBytesPerSecond() {
        return perSecond(admittedBytes - reclaimedBytes);
    }

    /** Returns what the Delta holds for {@code chunkIndex}, empty when it holds nothing. */
    public ChunkDelta chunk(long chunkIndex) {
        if (chunkIndex < 0) {
            throw new IllegalArgumentException("Chunk must be non-negative: " + chunkIndex);
        }
        ChunkDelta delta = chunks.get(chunkIndex);
        return delta == null ? new ChunkDelta(chunkIndex, 0, 0, 0) : delta;
    }

    /** Returns every patched chunk, by ascending chunk index. */
    public List<ChunkDelta> chunks() {
        return Collections.unmodifiableList(new ArrayList<>(chunks.values()));
    }

    /** Returns the indexes of the patched chunks, ascending. */
    public long[] chunkIndexes() {
        long[] indexes = new long[chunks.size()];
        int position = 0;
        for (Long index : chunks.keySet()) {
            indexes[position++] = index;
        }
        return indexes;
    }

    /** Returns what the Delta holds for {@code vertex}, empty when it holds nothing. */
    public VertexDelta vertex(long vertex) {
        for (VertexDelta candidate : vertices) {
            if (candidate.vertex() == vertex) {
                return candidate;
            }
        }
        return new VertexDelta(vertex, 0, 0);
    }

    /** Returns every patched vertex, heaviest first, which is the order a hot set is read in. */
    public List<VertexDelta> vertices() {
        return vertices;
    }

    /**
     * Returns how concentrated Delta bytes are over patched vertices. This is the measurement the
     * hot-set policy is set from: whether a small share of vertices really does hold most of the
     * Delta is answered here, not assumed.
     */
    public ConcentrationProfile vertexBytesConcentration() {
        long[] values = new long[vertices.size()];
        for (int index = 0; index < values.length; index++) {
            values[index] = vertices.get(index).bytes();
        }
        return ConcentrationProfile.of(values);
    }

    /** Returns how concentrated Delta bytes are over the patched chunks. */
    public ConcentrationProfile chunkBytesConcentration() {
        long[] values = new long[chunks.size()];
        int position = 0;
        for (ChunkDelta delta : chunks.values()) {
            values[position++] = delta.bytes();
        }
        return ConcentrationProfile.of(values);
    }

    private double perSecond(long amount) {
        if (amount == 0) {
            return 0.0d;
        }
        double seconds = observedWindow.toNanos() / 1_000_000_000.0d;
        if (seconds <= 0.0d) {
            return amount > 0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
        }
        return amount / seconds;
    }
}
