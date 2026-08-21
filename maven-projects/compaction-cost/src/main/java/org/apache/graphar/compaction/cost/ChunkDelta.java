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
 * What the Delta currently holds against one Base chunk.
 *
 * <p>This is the aggregation compaction is decided over, because a chunk is the unit that gets
 * rewritten: the patches of a chunk are exactly the Delta a rewrite of that chunk would eliminate.
 */
public final class ChunkDelta {
    private final long chunkIndex;
    private final long patchedVertices;
    private final long entries;
    private final long bytes;

    ChunkDelta(long chunkIndex, long patchedVertices, long entries, long bytes) {
        this.chunkIndex = chunkIndex;
        this.patchedVertices = patchedVertices;
        this.entries = entries;
        this.bytes = bytes;
    }

    /** Returns the chunk these patches belong to. */
    public long chunkIndex() {
        return chunkIndex;
    }

    /** Returns how many distinct vertices of the chunk carry patches. */
    public long patchedVertices() {
        return patchedVertices;
    }

    /** Returns the adjacency entries the Delta holds for the chunk. */
    public long entries() {
        return entries;
    }

    /** Returns the bytes the Delta holds for the chunk. */
    public long bytes() {
        return bytes;
    }

    /** Returns whether the Delta holds nothing for the chunk. */
    public boolean isEmpty() {
        return entries == 0 && bytes == 0 && patchedVertices == 0;
    }

    @Override
    public String toString() {
        return "ChunkDelta{chunk="
                + chunkIndex
                + ", vertices="
                + patchedVertices
                + ", entries="
                + entries
                + ", bytes="
                + bytes
                + "}";
    }
}
