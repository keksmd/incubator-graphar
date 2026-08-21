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
 * What the Delta currently holds for one vertex.
 *
 * <p>Vertex identity is kept, not only aggregated away into chunk totals, because the retention
 * policy is stated over vertices: a small set of hot vertices may be worth leaving in the Delta
 * permanently, and naming them is the first thing a policy needs.
 */
public final class VertexDelta {
    private final long vertex;
    private final long entries;
    private final long bytes;

    VertexDelta(long vertex, long entries, long bytes) {
        this.vertex = vertex;
        this.entries = entries;
        this.bytes = bytes;
    }

    /** Returns the patched vertex. */
    public long vertex() {
        return vertex;
    }

    /** Returns the adjacency entries the Delta holds for it. */
    public long entries() {
        return entries;
    }

    /** Returns the bytes the Delta holds for it. */
    public long bytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return "VertexDelta{vertex=" + vertex + ", entries=" + entries + ", bytes=" + bytes + "}";
    }
}
