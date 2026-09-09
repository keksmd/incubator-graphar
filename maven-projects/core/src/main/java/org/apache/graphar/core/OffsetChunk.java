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

package org.apache.graphar.core;

import java.util.Arrays;
import java.util.Objects;

/** A validated ordered-layout offset chunk with one more value than local vertices. */
public final class OffsetChunk {
    private final long vertexChunkIndex;
    private final long[] offsets;

    private OffsetChunk(long vertexChunkIndex, long[] offsets) {
        this.vertexChunkIndex = vertexChunkIndex;
        this.offsets = offsets;
    }

    /**
     * Creates an immutable offset chunk read from the offset file of {@code vertexChunkIndex}.
     * Values must start at zero and be non-negative and monotonic.
     */
    public static OffsetChunk of(long vertexChunkIndex, long[] offsets) {
        if (vertexChunkIndex < 0) {
            throw new IllegalArgumentException(
                    "Vertex chunk index must be non-negative: " + vertexChunkIndex);
        }
        Objects.requireNonNull(offsets, "Offset values cannot be null.");
        if (offsets.length < 2) {
            throw new IllegalArgumentException("An offset chunk must contain at least two values.");
        }
        long[] copy = Arrays.copyOf(offsets, offsets.length);
        if (copy[0] != 0) {
            throw new IllegalArgumentException("The first offset value must be zero: " + copy[0]);
        }
        long previous = copy[0];
        for (int index = 1; index < copy.length; index++) {
            long current = copy[index];
            if (current < previous) {
                throw new IllegalArgumentException(
                        "Offset values must be monotonic at index "
                                + index
                                + ": "
                                + previous
                                + " > "
                                + current);
            }
            previous = current;
        }
        return new OffsetChunk(vertexChunkIndex, copy);
    }

    /** Returns the vertex chunk this offset chunk was read from. */
    public long vertexChunkIndex() {
        return vertexChunkIndex;
    }

    /** Returns the number of local vertices represented by this chunk. */
    public long vertexCount() {
        return offsets.length - 1L;
    }

    /** Resolves the half-open edge range for one local vertex position. */
    public EdgeRange rangeFor(long localVertexIndex) {
        if (localVertexIndex < 0 || localVertexIndex >= vertexCount()) {
            throw new IllegalArgumentException(
                    "Local vertex index must be in [0, "
                            + vertexCount()
                            + "): "
                            + localVertexIndex);
        }
        int index = Math.toIntExact(localVertexIndex);
        return EdgeRange.fromOffsets(offsets[index], offsets[index + 1]);
    }

    /** Fails if the final offset does not equal the corresponding partition edge count. */
    public void validateEdgeCount(long edgeCount) {
        if (edgeCount < 0) {
            throw new IllegalArgumentException("Edge count must be non-negative: " + edgeCount);
        }
        long finalOffset = offsets[offsets.length - 1];
        if (finalOffset != edgeCount) {
            throw new IllegalArgumentException(
                    "Final offset must equal edge count: " + finalOffset + " != " + edgeCount);
        }
    }

    @Override
    public String toString() {
        return "OffsetChunk{vertexChunk="
                + vertexChunkIndex
                + ", vertexCount="
                + vertexCount()
                + ", edgeCount="
                + offsets[offsets.length - 1]
                + "}";
    }
}
