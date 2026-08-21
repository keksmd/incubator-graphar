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

/**
 * One ingest group: the arriving edges a classification routed the same way.
 *
 * <p>The endpoint arrays are exact length and are handed out rather than copied. A group is
 * produced once per ingest batch and consumed once, by a merge or by a delta write, and both take
 * endpoint arrays; copying here would double the per-batch cost that classifying cheaply exists to
 * avoid. Callers must not mutate them.
 */
public final class EdgeArrivals {
    private final long[] sources;
    private final long[] targets;

    EdgeArrivals(long[] sources, long[] targets) {
        this.sources = sources;
        this.targets = targets;
    }

    /** Returns the number of edges in this group. */
    public int count() {
        return sources.length;
    }

    /** Returns whether this group holds no edges. */
    public boolean isEmpty() {
        return sources.length == 0;
    }

    /** Returns the source endpoints, in arrival order, without copying. */
    public long[] sources() {
        return sources;
    }

    /** Returns the target endpoints, in arrival order, without copying. */
    public long[] targets() {
        return targets;
    }

    /** Returns the source endpoint of one arrival. */
    public long source(int index) {
        return sources[checkedIndex(index)];
    }

    /** Returns the target endpoint of one arrival. */
    public long target(int index) {
        return targets[checkedIndex(index)];
    }

    private int checkedIndex(int index) {
        if (index < 0 || index >= sources.length) {
            throw new IllegalArgumentException(
                    "Arrival index must be in [0, " + sources.length + "): " + index);
        }
        return index;
    }
}
