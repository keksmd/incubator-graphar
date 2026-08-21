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

import java.io.Closeable;
import java.io.IOException;

/**
 * The durable record of everything a delta was told, in the order it was told.
 *
 * <p>The delta is the only place where the seconds-fresh part of the graph exists: the base was
 * written before those patches arrived and does not contain them. Losing the delta therefore means
 * losing acknowledged data until the next full rebuild, which is exactly the hour-scale gap the
 * delta exists to close. A delta with no journal is legitimate only when its contents can be
 * replayed from somewhere else.
 *
 * <p>The contract is an ordered log, not a store: a vertex allocation and an edge are appended in
 * the order the delta accepted them, and {@link #replay} hands them back in that order, which is
 * what makes a vertex number reproducible after a restart. Durability is the caller's choice of
 * cadence through {@link #sync()}: an append is not durable until a sync that follows it returns.
 */
public interface DeltaJournal extends Closeable {
    /** Returns a journal that records nothing, for a delta whose contents are replayable. */
    static DeltaJournal none() {
        return new NoDeltaJournal();
    }

    /** Returns the first edge sequence this journal holds, which a rebase moves forward. */
    long edgeFloor();

    /** Records that a vertex of {@code typeOrdinal} entered the delta under {@code externalId}. */
    void vertex(int typeOrdinal, String externalId) throws IOException;

    /** Records one edge between two global vertex numbers. */
    void edge(int source, int target) throws IOException;

    /** Makes every append made so far durable. */
    void sync() throws IOException;

    /** Replays what this journal holds, oldest first. */
    void replay(Visitor visitor) throws IOException;

    /**
     * Replaces the contents of this journal with what {@code content} writes, durably.
     *
     * <p>This is how a compaction stops paying for what the base has absorbed: the edges below the
     * new floor are gone from the log as well as from memory.
     */
    void rewrite(long baseVertexCount, long edgeFloor, Content content) throws IOException;

    /** Receives what a journal holds during {@link #replay}. */
    interface Visitor {
        /** Receives one vertex allocation. */
        void vertex(int typeOrdinal, String externalId);

        /** Receives one edge. */
        void edge(int source, int target);
    }

    /** Writes the surviving contents of a journal during {@link #rewrite}. */
    @FunctionalInterface
    interface Content {
        /** Appends everything that survives into {@code journal}. */
        void writeTo(DeltaJournal journal) throws IOException;
    }
}
