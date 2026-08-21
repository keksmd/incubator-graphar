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
 * What an unmerged Delta costs one read, expressed as {@code read_penalty(delta)}.
 *
 * <p>The penalty is stated per read and in the units a reader actually pays: how often a read meets
 * a patched vertex at all, and how much extra adjacency it has to merge when it does. The constant
 * cost of probing the Delta once per read is deliberately absent - that is a latency of whichever
 * structure the Delta is built on, measurable only there - so what this class reports is the part
 * that follows from the shape of the Delta itself.
 *
 * <p>{@link #readAmplification()} puts the same numbers against Base, which is the bounded quantity
 * a serving contract is written in.
 */
public final class ReadPenalty {
    private final double probabilityOfPatchedRead;
    private final double expectedExtraEntriesPerRead;
    private final double expectedExtraBytesPerRead;
    private final double expectedBaseEntriesPerRead;

    ReadPenalty(
            double probabilityOfPatchedRead,
            double expectedExtraEntriesPerRead,
            double expectedExtraBytesPerRead,
            double expectedBaseEntriesPerRead) {
        this.probabilityOfPatchedRead = probabilityOfPatchedRead;
        this.expectedExtraEntriesPerRead = expectedExtraEntriesPerRead;
        this.expectedExtraBytesPerRead = expectedExtraBytesPerRead;
        this.expectedBaseEntriesPerRead = expectedBaseEntriesPerRead;
    }

    /** Returns the share of reads that land on a vertex the Delta holds patches for. */
    public double probabilityOfPatchedRead() {
        return probabilityOfPatchedRead;
    }

    /** Returns the adjacency entries an average read has to merge on top of Base. */
    public double expectedExtraEntriesPerRead() {
        return expectedExtraEntriesPerRead;
    }

    /** Returns the Delta bytes an average read has to touch on top of Base. */
    public double expectedExtraBytesPerRead() {
        return expectedExtraBytesPerRead;
    }

    /** Returns the Base adjacency entries an average read touches. */
    public double expectedBaseEntriesPerRead() {
        return expectedBaseEntriesPerRead;
    }

    /**
     * Returns entries read from {@code Base + Delta} over entries read from Base alone: one when
     * the Delta is empty, and {@link Double#POSITIVE_INFINITY} when reads land where Base holds
     * nothing and the Delta holds something.
     */
    public double readAmplification() {
        if (expectedBaseEntriesPerRead == 0.0d) {
            return expectedExtraEntriesPerRead == 0.0d ? 1.0d : Double.POSITIVE_INFINITY;
        }
        return (expectedBaseEntriesPerRead + expectedExtraEntriesPerRead)
                / expectedBaseEntriesPerRead;
    }

    @Override
    public String toString() {
        return "ReadPenalty{patchedReadShare="
                + probabilityOfPatchedRead
                + ", extraEntriesPerRead="
                + expectedExtraEntriesPerRead
                + ", extraBytesPerRead="
                + expectedExtraBytesPerRead
                + ", readAmplification="
                + readAmplification()
                + "}";
    }
}
