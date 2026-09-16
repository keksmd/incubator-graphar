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

/** Long-safe operations on GraphAr vertex and edge chunks. */
public final class ChunkMath {
    private ChunkMath() {}

    /**
     * Returns the chunk containing a non-negative element identifier.
     *
     * @param elementId an element identifier
     * @param chunkSize a positive chunk size
     * @return the zero-based chunk index
     */
    public static long chunkIndex(long elementId, long chunkSize) {
        validateElementId(elementId);
        validateChunkSize(chunkSize);
        return elementId / chunkSize;
    }

    /**
     * Returns an element's zero-based position inside its chunk.
     *
     * @param elementId an element identifier
     * @param chunkSize a positive chunk size
     * @return the zero-based offset within the chunk
     */
    public static long offsetInChunk(long elementId, long chunkSize) {
        validateElementId(elementId);
        validateChunkSize(chunkSize);
        return elementId % chunkSize;
    }

    /**
     * Returns the number of chunks required for a non-negative number of elements.
     *
     * @param elementCount a number of elements
     * @param chunkSize a positive chunk size
     * @return the number of chunks needed to contain the elements
     */
    public static long chunkCount(long elementCount, long chunkSize) {
        if (elementCount < 0) {
            throw new IllegalArgumentException(
                    "Element count must be non-negative: " + elementCount);
        }
        validateChunkSize(chunkSize);
        return elementCount == 0 ? 0 : 1 + (elementCount - 1) / chunkSize;
    }

    static void validateElementId(long elementId) {
        if (elementId < 0) {
            throw new IllegalArgumentException("Element ID must be non-negative: " + elementId);
        }
    }

    static void validateChunkSize(long chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be positive: " + chunkSize);
        }
    }
}
