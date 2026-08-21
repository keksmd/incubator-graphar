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

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.graphar.core.ChunkMath;

/**
 * Records what the mutable Delta holds, so compaction can be decided by arithmetic instead of by a
 * timer.
 *
 * <p>The Delta calls {@link #patched} for every adjacency entry it accepts and {@link #reclaim}
 * once a chunk has been folded into Base. From that stream this class keeps the quantities no one
 * can reconstruct afterwards: which vertices are patched, how much each of them holds, when the
 * accumulation started, and how much has already been given back. Everything the decision needs is
 * then read from a {@link #snapshot()}, which is immutable and therefore safe to reason over while
 * ingest continues.
 *
 * <p>A patch is recorded per <em>affected endpoint</em>. An undirected or doubly materialized
 * projection stores an edge under both of its endpoints, so such an edge is two patches against
 * possibly two different chunks; deciding which endpoints an arriving edge affects belongs to the
 * Delta, which knows the physical ordering, not to the accounting.
 *
 * <p>Byte counts are the Delta's own: it reports what holding the patch costs it, including its
 * per-entry overhead. This class never converts entries into bytes with a constant of its own.
 *
 * <p>Instances are safe to share between an ingesting thread and a deciding thread.
 */
public final class DeltaCostLedger {
    private final long vertexChunkSize;
    private final Clock clock;
    private final Map<Long, long[]> patches = new HashMap<>();
    private long admittedEntries;
    private long admittedBytes;
    private long reclaimedEntries;
    private long reclaimedBytes;
    private Instant firstArrival;
    private Instant lastArrival;

    /** Creates a ledger for a projection whose chunks cover {@code vertexChunkSize} vertices. */
    public DeltaCostLedger(long vertexChunkSize) {
        this(vertexChunkSize, Clock.systemUTC());
    }

    /** Creates a ledger against an explicit clock, which is what makes growth rates testable. */
    public DeltaCostLedger(long vertexChunkSize, Clock clock) {
        if (vertexChunkSize <= 0) {
            throw new IllegalArgumentException(
                    "Vertex chunk size must be positive: " + vertexChunkSize);
        }
        this.vertexChunkSize = vertexChunkSize;
        this.clock = Objects.requireNonNull(clock, "Clock cannot be null.");
    }

    /** Returns the number of vertices one chunk covers. */
    public long vertexChunkSize() {
        return vertexChunkSize;
    }

    /**
     * Records that the Delta accepted {@code entries} adjacency entries for {@code vertex}, costing
     * it {@code bytes} to hold.
     */
    public synchronized void patched(long vertex, long entries, long bytes) {
        if (vertex < 0) {
            throw new IllegalArgumentException("Vertex must be non-negative: " + vertex);
        }
        if (entries < 0) {
            throw new IllegalArgumentException("Entries must be non-negative: " + entries);
        }
        if (bytes < 0) {
            throw new IllegalArgumentException("Bytes must be non-negative: " + bytes);
        }
        long[] held = patches.computeIfAbsent(vertex, key -> new long[2]);
        held[0] += entries;
        held[1] += bytes;
        admittedEntries += entries;
        admittedBytes += bytes;
        Instant now = clock.instant();
        if (firstArrival == null) {
            firstArrival = now;
        }
        lastArrival = now;
    }

    /**
     * Drops everything the Delta held for {@code chunkIndex} and returns it.
     *
     * <p>This is what a completed chunk compaction reports back: the patches are now in Base, so
     * they stop counting as Delta storage and as read penalty, while the arrival history that
     * produced them stays in the growth rate.
     */
    public synchronized ChunkDelta reclaim(long chunkIndex) {
        if (chunkIndex < 0) {
            throw new IllegalArgumentException("Chunk must be non-negative: " + chunkIndex);
        }
        long vertices = 0;
        long entries = 0;
        long bytes = 0;
        for (Map.Entry<Long, long[]> patch : patches.entrySet()) {
            if (ChunkMath.chunkIndex(patch.getKey(), vertexChunkSize) != chunkIndex) {
                continue;
            }
            vertices++;
            entries += patch.getValue()[0];
            bytes += patch.getValue()[1];
        }
        patches.keySet()
                .removeIf(vertex -> ChunkMath.chunkIndex(vertex, vertexChunkSize) == chunkIndex);
        reclaimedEntries += entries;
        reclaimedBytes += bytes;
        return new ChunkDelta(chunkIndex, vertices, entries, bytes);
    }

    /**
     * Drops everything the Delta held for one vertex and returns it, which is how a patch pulled
     * into Base on its own leaves the accounting.
     */
    public synchronized VertexDelta forget(long vertex) {
        if (vertex < 0) {
            throw new IllegalArgumentException("Vertex must be non-negative: " + vertex);
        }
        long[] held = patches.remove(vertex);
        if (held == null) {
            return new VertexDelta(vertex, 0, 0);
        }
        reclaimedEntries += held[0];
        reclaimedBytes += held[1];
        return new VertexDelta(vertex, held[0], held[1]);
    }

    /** Returns an immutable readout of everything recorded so far. */
    public synchronized DeltaCostSnapshot snapshot() {
        Map<Long, long[]> copy = new HashMap<>(patches.size());
        for (Map.Entry<Long, long[]> patch : patches.entrySet()) {
            copy.put(patch.getKey(), patch.getValue().clone());
        }
        return new DeltaCostSnapshot(
                vertexChunkSize,
                copy,
                admittedEntries,
                admittedBytes,
                reclaimedEntries,
                reclaimedBytes,
                firstArrival,
                lastArrival,
                clock.instant());
    }
}
