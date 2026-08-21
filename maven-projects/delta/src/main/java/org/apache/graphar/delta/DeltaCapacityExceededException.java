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
 * Thrown when a delta has reached the edge ceiling of its {@link DeltaOptions}.
 *
 * <p>This is not a failure of the batch that hit it. It is the signal that the delta has stopped
 * being a cheap overlay on the base and has to be folded into it: the host answers by compacting
 * the chunks the delta touches and rebasing, after which the same batch is accepted.
 */
public final class DeltaCapacityExceededException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final long edgeCount;
    private final long maxEdges;

    DeltaCapacityExceededException(long edgeCount, long maxEdges) {
        super(
                "Delta holds "
                        + edgeCount
                        + " edges and may hold "
                        + maxEdges
                        + "; compact it into the base before appending more.");
        this.edgeCount = edgeCount;
        this.maxEdges = maxEdges;
    }

    /** Returns the number of edges the delta held when it refused. */
    public long edgeCount() {
        return edgeCount;
    }

    /** Returns the ceiling that was reached. */
    public long maxEdges() {
        return maxEdges;
    }
}
