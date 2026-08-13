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

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds one materialized projection and replaces it with a freshly built one in a single reference
 * assignment.
 *
 * <p>A dataset in object storage is rebuilt periodically, while requests are served from the
 * in-memory adjacency. Both facts together mean the serving side needs a rebuild that never exposes
 * a half-built graph and never blocks a reader. This class provides exactly that: {@link
 * #current()} hands out an immutable {@link Snapshot} that stays valid for the whole request even
 * if a rebuild finishes in the middle of it, and {@link #refresh()} publishes a new snapshot only
 * after the new projection has been built in full.
 *
 * <p>A failed rebuild leaves the previous snapshot in place and propagates the failure, so a broken
 * dataset degrades to stale answers rather than to no answers. Staleness is observable through
 * {@link Snapshot#age(Clock)} and {@link #isStale(Duration)}, which is what an API contract with a
 * maximum index age is checked against.
 *
 * <p>This class owns no threads. The caller decides when to rebuild, because a library that starts
 * its own scheduler cannot be shut down cleanly by its host.
 */
public final class GraphProjection {
    private final ProjectionBuilder builder;
    private final Clock clock;
    private final AtomicReference<Snapshot> snapshot = new AtomicReference<>();
    private final AtomicBoolean refreshing = new AtomicBoolean();

    private GraphProjection(ProjectionBuilder builder, Clock clock, Snapshot initial) {
        this.builder = builder;
        this.clock = clock;
        this.snapshot.set(initial);
    }

    /** Builds the first projection and returns a holder that serves it. */
    public static GraphProjection load(ProjectionBuilder builder) throws IOException {
        return load(builder, Clock.systemUTC());
    }

    /** Builds the first projection against an explicit clock. */
    public static GraphProjection load(ProjectionBuilder builder, Clock clock) throws IOException {
        Objects.requireNonNull(builder, "Projection builder cannot be null.");
        Objects.requireNonNull(clock, "Clock cannot be null.");
        Instant startedAt = clock.instant();
        HeterogeneousCsr csr =
                Objects.requireNonNull(builder.build(), "Projection builder returned null.");
        return new GraphProjection(builder, clock, new Snapshot(csr, startedAt, 1L));
    }

    /**
     * Returns the snapshot currently being served. Answer a whole request from the returned value:
     * calling this twice within one request can straddle a refresh and mix two graphs.
     */
    public Snapshot current() {
        return snapshot.get();
    }

    /**
     * Builds a new projection and publishes it, returning the published snapshot.
     *
     * <p>The build runs on the calling thread and readers keep serving the previous snapshot until
     * it completes. Concurrent refreshes are collapsed: if another thread is already rebuilding,
     * this returns the snapshot in force without starting a second build.
     */
    public Snapshot refresh() throws IOException {
        if (!refreshing.compareAndSet(false, true)) {
            return snapshot.get();
        }
        try {
            Instant startedAt = clock.instant();
            HeterogeneousCsr csr =
                    Objects.requireNonNull(builder.build(), "Projection builder returned null.");
            Snapshot published = new Snapshot(csr, startedAt, snapshot.get().generation() + 1L);
            snapshot.set(published);
            return published;
        } finally {
            refreshing.set(false);
        }
    }

    /**
     * Publishes a projection the caller derived, returning the published snapshot.
     *
     * <p>A dataset under continuous ingest grows by edges that a full rebuild would derive the
     * unchanged part of again. {@link HeterogeneousCsr#merge} produces the extended projection
     * without reading the dataset; this makes it the served one under the same rule a rebuild
     * follows, so readers holding the previous snapshot keep answering from it.
     *
     * @throws IllegalStateException when a rebuild is in flight, because publishing over it would
     *     decide the order of two projections by which one finished first
     */
    public Snapshot publish(HeterogeneousCsr projection) {
        Objects.requireNonNull(projection, "Published projection cannot be null.");
        if (!refreshing.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "A rebuild is in flight; publish a derived projection outside one.");
        }
        try {
            Snapshot published =
                    new Snapshot(projection, clock.instant(), snapshot.get().generation() + 1L);
            snapshot.set(published);
            return published;
        } finally {
            refreshing.set(false);
        }
    }

    /** Refreshes only when the served snapshot is older than {@code maxAge}. */
    public Snapshot refreshIfStale(Duration maxAge) throws IOException {
        return isStale(maxAge) ? refresh() : snapshot.get();
    }

    /** Reports whether the served snapshot is older than {@code maxAge}. */
    public boolean isStale(Duration maxAge) {
        Objects.requireNonNull(maxAge, "Maximum age cannot be null.");
        if (maxAge.isNegative()) {
            throw new IllegalArgumentException("Maximum age cannot be negative: " + maxAge);
        }
        return snapshot.get().age(clock).compareTo(maxAge) > 0;
    }

    /** Builds one projection from the dataset. */
    @FunctionalInterface
    public interface ProjectionBuilder {
        /** Reads the dataset and materializes a projection. */
        HeterogeneousCsr build() throws IOException;
    }

    /** One immutable projection together with the instant its build started. */
    public static final class Snapshot {
        private final HeterogeneousCsr projection;
        private final Instant builtAt;
        private final long generation;

        private Snapshot(HeterogeneousCsr projection, Instant builtAt, long generation) {
            this.projection = projection;
            this.builtAt = builtAt;
            this.generation = generation;
        }

        /** Returns the merged projection. */
        public HeterogeneousCsr projection() {
            return projection;
        }

        /**
         * Returns the instant the build started, not the instant it finished, so the reported age
         * is never younger than the data.
         */
        public Instant builtAt() {
            return builtAt;
        }

        /** Returns the publication number, which increases by one on every published refresh. */
        public long generation() {
            return generation;
        }

        /** Returns how long ago the build of this snapshot started. */
        public Duration age(Clock clock) {
            Duration age = Duration.between(builtAt, clock.instant());
            return age.isNegative() ? Duration.ZERO : age;
        }
    }
}
