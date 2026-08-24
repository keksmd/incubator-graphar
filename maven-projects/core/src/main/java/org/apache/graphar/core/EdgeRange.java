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

/** A validated half-open range of edge rows within one GraphAr vertex partition. */
public final class EdgeRange {
    private final long begin;
    private final long end;

    private EdgeRange(long begin, long end) {
        this.begin = begin;
        this.end = end;
    }

    /**
     * Creates an edge range from the two adjacent values of an ordered offset table.
     *
     * @param begin the first included edge row
     * @param end the first excluded edge row
     * @return the validated half-open edge range
     */
    public static EdgeRange fromOffsets(long begin, long end) {
        if (begin < 0) {
            throw new IllegalArgumentException("Edge range begin must be non-negative: " + begin);
        }
        if (end < begin) {
            throw new IllegalArgumentException(
                    "Offset values must be monotonic: begin=" + begin + ", end=" + end);
        }
        return new EdgeRange(begin, end);
    }

    /**
     * Returns the first included edge row.
     *
     * @return the first included edge row
     */
    public long begin() {
        return begin;
    }

    /**
     * Returns the first excluded edge row.
     *
     * @return the first excluded edge row
     */
    public long end() {
        return end;
    }

    /**
     * Returns the number of selected edge rows.
     *
     * @return the number of selected edge rows
     */
    public long length() {
        return end - begin;
    }

    /**
     * Returns whether this range selects no edge rows.
     *
     * @return whether the range is empty
     */
    public boolean isEmpty() {
        return begin == end;
    }

    /**
     * Returns the half-open range of edge chunks intersecting this edge range.
     *
     * @param edgeChunkSize a positive number of edge rows per chunk
     * @return the chunk range intersecting this edge range
     */
    public ChunkRange edgeChunks(long edgeChunkSize) {
        ChunkMath.validateChunkSize(edgeChunkSize);
        long first = begin / edgeChunkSize;
        if (isEmpty()) {
            return new ChunkRange(first, first);
        }
        return new ChunkRange(first, 1 + (end - 1) / edgeChunkSize);
    }
}
