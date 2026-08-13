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

package org.apache.graphar.reader;

/**
 * The size a heap CSR can reach, and the arithmetic to check a graph against it before building.
 *
 * <p>The projection is held in Java arrays, so it is bounded twice: by the array index type, and by
 * the heap the process was given. Both bounds are silent by default. Exceeding the first raises an
 * {@code ArithmeticException} that names an integer overflow rather than the graph, and exceeding
 * the second raises an {@code OutOfMemoryError} halfway through a build. This class turns both into
 * a statement of what was attempted and what the limit is, checked before any array is allocated.
 *
 * <p>The numbers are exact, not estimates: a CSR of {@code v} vertices and {@code e} adjacency
 * entries occupies {@code 4 * (v + 1)} bytes of offsets and {@code 4 * e} bytes of destinations,
 * because a projection this class admits is one whose identifiers fit an array index. An undirected
 * projection stores each edge twice, so its entry count is twice its edge count.
 */
public final class ProjectionCapacity {
    /** The largest number of vertices a heap CSR can address. */
    public static final long MAX_VERTICES = Integer.MAX_VALUE - 9L;

    /** The largest number of adjacency entries a heap CSR can address. */
    public static final long MAX_ENTRIES = Integer.MAX_VALUE - 8L;

    private static final long BYTES_PER_SLOT = Integer.BYTES;

    private ProjectionCapacity() {}

    /**
     * Returns the entry count a projection of {@code edgeCount} edges stores in {@code direction}.
     */
    public static long entryCount(long edgeCount, CsrDirection direction) {
        return direction == CsrDirection.UNDIRECTED ? Math.multiplyExact(edgeCount, 2L) : edgeCount;
    }

    /** Returns the heap the CSR arrays of a graph of this size occupy. */
    public static long heapBytes(long vertexCount, long entryCount) {
        return Math.addExact(
                Math.multiplyExact(Math.addExact(vertexCount, 1L), BYTES_PER_SLOT),
                Math.multiplyExact(entryCount, BYTES_PER_SLOT));
    }

    /**
     * Returns the heap a one-scan build of this size peaks at, which includes the endpoint buffers
     * a merged projection fills before placing entries, and the offset cursor the placement copies.
     */
    public static long peakBuildBytes(long vertexCount, long edgeCount, CsrDirection direction) {
        long endpoints = Math.multiplyExact(Math.multiplyExact(edgeCount, 2L), BYTES_PER_SLOT);
        return Math.addExact(twoScanPeakBuildBytes(vertexCount, edgeCount, direction), endpoints);
    }

    /**
     * Returns the heap a two-scan build of this size peaks at. Reading the topology a second time
     * replaces the endpoint buffers, so this is the least heap in which the graph can be built at
     * all, and it is what {@link #requireHeadroom} holds a caller to.
     */
    public static long twoScanPeakBuildBytes(
            long vertexCount, long edgeCount, CsrDirection direction) {
        long entries = entryCount(edgeCount, direction);
        long cursor = Math.multiplyExact(Math.addExact(vertexCount, 1L), BYTES_PER_SLOT);
        return Math.addExact(heapBytes(vertexCount, entries), cursor);
    }

    /**
     * Rejects a graph that cannot be addressed by a heap CSR, naming the graph and the limit.
     *
     * @throws ProjectionTooLargeException when the graph is past {@link #MAX_VERTICES} or {@link
     *     #MAX_ENTRIES}
     */
    public static void requireAddressable(long vertexCount, long entryCount) {
        if (vertexCount < 0 || entryCount < 0) {
            throw new IllegalArgumentException(
                    "Graph size cannot be negative: vertices="
                            + vertexCount
                            + ", entries="
                            + entryCount);
        }
        if (vertexCount > MAX_VERTICES) {
            throw new ProjectionTooLargeException(
                    "Projection needs "
                            + vertexCount
                            + " vertices, but a heap CSR addresses at most "
                            + MAX_VERTICES
                            + ". Partition the graph or hold it outside the heap.");
        }
        if (entryCount > MAX_ENTRIES) {
            throw new ProjectionTooLargeException(
                    "Projection needs "
                            + entryCount
                            + " adjacency entries, but a heap CSR addresses at most "
                            + MAX_ENTRIES
                            + ". Partition the graph or hold it outside the heap.");
        }
    }

    /**
     * Rejects a build that cannot fit in {@code availableBytes} by any strategy, naming both
     * numbers, so the caller fails before allocating instead of during it.
     *
     * @throws ProjectionTooLargeException when even a two-scan build is past the available heap
     */
    public static void requireHeadroom(
            long vertexCount, long edgeCount, CsrDirection direction, long availableBytes) {
        long required = twoScanPeakBuildBytes(vertexCount, edgeCount, direction);
        if (required > availableBytes) {
            throw new ProjectionTooLargeException(
                    "Projection of "
                            + vertexCount
                            + " vertices and "
                            + edgeCount
                            + " edges needs about "
                            + required
                            + " bytes to build, but only "
                            + availableBytes
                            + " bytes are available. Raise the heap or partition the graph.");
        }
    }
}
