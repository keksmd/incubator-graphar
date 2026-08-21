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

package org.apache.graphar.compaction;

import java.util.Objects;
import org.apache.graphar.info.type.AdjListType;

/**
 * One vertex-aligned partition of one adjacency layout, identified independently of any dataset.
 *
 * <p>This is the unit a compaction decision is made about. GraphAr keeps the offsets, the adjacency
 * chunk sequence, and the edge count of a partition inside that partition, so a partition is the
 * smallest part of a layout that can be recomputed without touching the rest of it. Everything a
 * cost model weighs about a rewrite -- how many patches are pending for it, how many bytes it
 * moves, how much of the Delta it drains -- is weighed per instance of this class.
 */
public final class EdgeChunk {
    private final String srcType;
    private final String edgeType;
    private final String dstType;
    private final AdjListType layout;
    private final long partition;

    /** Creates a reference to one vertex-aligned partition of one edge triplet's layout. */
    public EdgeChunk(
            String srcType, String edgeType, String dstType, AdjListType layout, long partition) {
        this.srcType = Objects.requireNonNull(srcType, "Source vertex type cannot be null.");
        this.edgeType = Objects.requireNonNull(edgeType, "Edge type cannot be null.");
        this.dstType = Objects.requireNonNull(dstType, "Destination vertex type cannot be null.");
        this.layout = Objects.requireNonNull(layout, "Adjacency layout cannot be null.");
        if (partition < 0) {
            throw new IllegalArgumentException("Edge partition cannot be negative: " + partition);
        }
        this.partition = partition;
    }

    /** Returns the source vertex type of the edge triplet. */
    public String srcType() {
        return srcType;
    }

    /** Returns the edge type of the edge triplet. */
    public String edgeType() {
        return edgeType;
    }

    /** Returns the destination vertex type of the edge triplet. */
    public String dstType() {
        return dstType;
    }

    /** Returns the adjacency layout this partition belongs to. */
    public AdjListType layout() {
        return layout;
    }

    /** Returns the index of the vertex-aligned partition. */
    public long partition() {
        return partition;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof EdgeChunk)) {
            return false;
        }
        EdgeChunk that = (EdgeChunk) other;
        return partition == that.partition
                && layout == that.layout
                && srcType.equals(that.srcType)
                && edgeType.equals(that.edgeType)
                && dstType.equals(that.dstType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcType, edgeType, dstType, layout, partition);
    }

    @Override
    public String toString() {
        return srcType + "_" + edgeType + "_" + dstType + "/" + layout + "/part" + partition;
    }
}
