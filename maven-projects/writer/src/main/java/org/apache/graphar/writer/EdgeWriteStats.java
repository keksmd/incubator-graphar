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

package org.apache.graphar.writer;

/** Measured resource bounds and output cardinality from one streamed edge-layout write. */
public final class EdgeWriteStats {
    private final long edgeCount;
    private final long partitionCount;
    private final long spillRunCount;
    private final int peakRecordsBuffered;

    EdgeWriteStats(
            long edgeCount, long partitionCount, long spillRunCount, int peakRecordsBuffered) {
        this.edgeCount = edgeCount;
        this.partitionCount = partitionCount;
        this.spillRunCount = spillRunCount;
        this.peakRecordsBuffered = peakRecordsBuffered;
    }

    /** Returns the number of edge rows published. */
    public long edgeCount() {
        return edgeCount;
    }

    /** Returns the number of GraphAr vertex-aligned partitions written. */
    public long partitionCount() {
        return partitionCount;
    }

    /** Returns the number of bounded sorted runs created before merge. */
    public long spillRunCount() {
        return spillRunCount;
    }

    /** Returns the largest in-heap sort run observed during the write. */
    public int peakRecordsBuffered() {
        return peakRecordsBuffered;
    }
}
