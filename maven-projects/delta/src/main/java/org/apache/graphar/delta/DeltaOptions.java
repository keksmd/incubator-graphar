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

package org.apache.graphar.delta;

/**
 * The resource ceiling and allocation grain of one delta.
 *
 * <p>A delta that grows without a limit turns the read path it exists to make fast into the slowest
 * part of the system, because every read pays for every patch that was never folded into the base.
 * {@link #maxEdges()} is therefore a hard ceiling: reaching it is the signal that compaction is
 * overdue, and the delta refuses the edge rather than accepting an unbounded read penalty.
 *
 * <p>The ceiling is a host decision, not a property of the format. The default is deliberately
 * small enough to be felt in a test and large enough to cover a compaction interval measured in
 * minutes; a deployment picks its own from measured delta growth and read amplification.
 */
public final class DeltaOptions {
    private static final long DEFAULT_MAX_EDGES = 8_000_000L;
    private static final int DEFAULT_CHUNK_SIZE = 1 << 20;
    private static final int DEFAULT_VERTEX_CAPACITY = 1 << 14;

    private final long maxEdges;
    private final int chunkSize;
    private final int initialVertexCapacity;

    private DeltaOptions(long maxEdges, int chunkSize, int initialVertexCapacity) {
        this.maxEdges = maxEdges;
        this.chunkSize = chunkSize;
        this.initialVertexCapacity = initialVertexCapacity;
    }

    /** Returns the default ceiling and grain. */
    public static DeltaOptions defaults() {
        return new DeltaOptions(DEFAULT_MAX_EDGES, DEFAULT_CHUNK_SIZE, DEFAULT_VERTEX_CAPACITY);
    }

    /** Returns these options with a different ceiling on the edges the delta may hold. */
    public DeltaOptions maxEdges(long maxEdges) {
        if (maxEdges < 1) {
            throw new IllegalArgumentException("Delta edge ceiling must be positive: " + maxEdges);
        }
        if (maxEdges > (Integer.MAX_VALUE - 8L) / 2L) {
            throw new IllegalArgumentException(
                    "Delta edge ceiling is beyond what an entry index addresses: " + maxEdges);
        }
        return new DeltaOptions(maxEdges, chunkSize, initialVertexCapacity);
    }

    /**
     * Returns these options with a different allocation grain. The delta allocates storage one
     * chunk at a time, so the grain trades allocation count against the memory a barely used delta
     * holds.
     */
    public DeltaOptions chunkSize(int chunkSize) {
        if (chunkSize < 1 || Integer.bitCount(chunkSize) != 1) {
            throw new IllegalArgumentException(
                    "Delta chunk size must be a positive power of two: " + chunkSize);
        }
        return new DeltaOptions(maxEdges, chunkSize, initialVertexCapacity);
    }

    /** Returns these options with a different initial size of the patched-vertex table. */
    public DeltaOptions initialVertexCapacity(int initialVertexCapacity) {
        if (initialVertexCapacity < 1) {
            throw new IllegalArgumentException(
                    "Initial vertex capacity must be positive: " + initialVertexCapacity);
        }
        return new DeltaOptions(maxEdges, chunkSize, initialVertexCapacity);
    }

    /** Returns the largest number of edges the delta may hold before it refuses more. */
    public long maxEdges() {
        return maxEdges;
    }

    /** Returns the number of slots in one storage chunk. */
    public int chunkSize() {
        return chunkSize;
    }

    /** Returns the initial number of slots in the patched-vertex table. */
    public int initialVertexCapacity() {
        return initialVertexCapacity;
    }
}
