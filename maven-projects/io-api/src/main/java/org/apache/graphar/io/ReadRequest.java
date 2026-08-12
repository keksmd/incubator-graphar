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

package org.apache.graphar.io;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * An immutable physical read operation. Filters are combined with logical AND and may require a
 * backend to read columns that are not in the requested output projection.
 */
public final class ReadRequest {
    private final URI uri;
    private final Projection projection;
    private final RowRange rowRange;
    private final List<Filter> filters;
    private final Long limit;

    private ReadRequest(Builder builder) {
        this.uri = Objects.requireNonNull(builder.uri, "A read URI cannot be null.");
        this.projection = builder.projection;
        this.rowRange = builder.rowRange;
        this.filters = List.copyOf(builder.filters);
        this.limit = builder.limit;
    }

    /** Starts a request for {@code uri} with every available column selected. */
    public static Builder builder(URI uri) {
        return new Builder(uri);
    }

    /** Returns the logical location of the physical input. */
    public URI uri() {
        return uri;
    }

    /** Returns the requested output columns. */
    public Projection projection() {
        return projection;
    }

    /** Returns the optional half-open source-row range. */
    public Optional<RowRange> rowRange() {
        return Optional.ofNullable(rowRange);
    }

    /** Returns the immutable, ordered conjunction of filter hints. */
    public List<Filter> filters() {
        return filters;
    }

    /** Returns the optional maximum number of output rows; zero is a valid limit. */
    public OptionalLong limit() {
        return limit == null ? OptionalLong.empty() : OptionalLong.of(limit);
    }

    /** Returns every physical optimization requested by this operation. */
    public Set<ReadCapability> requestedCapabilities() {
        EnumSet<ReadCapability> capabilities = EnumSet.noneOf(ReadCapability.class);
        if (!projection.isAllColumns()) {
            capabilities.add(ReadCapability.PROJECTION);
        }
        if (rowRange != null) {
            capabilities.add(ReadCapability.ROW_RANGE);
        }
        if (!filters.isEmpty()) {
            capabilities.add(ReadCapability.FILTER);
        }
        if (limit != null) {
            capabilities.add(ReadCapability.LIMIT);
        }
        return Collections.unmodifiableSet(capabilities);
    }

    /** Builder for immutable {@link ReadRequest} values. */
    public static final class Builder {
        private final URI uri;
        private Projection projection = Projection.all();
        private RowRange rowRange;
        private List<Filter> filters = List.of();
        private Long limit;

        private Builder(URI uri) {
            this.uri = Objects.requireNonNull(uri, "A read URI cannot be null.");
        }

        /** Replaces the output projection. */
        public Builder projection(Projection projection) {
            this.projection = Objects.requireNonNull(projection, "A projection cannot be null.");
            return this;
        }

        /** Restricts the read to a half-open source-row range. */
        public Builder rowRange(RowRange rowRange) {
            this.rowRange = Objects.requireNonNull(rowRange, "A row range cannot be null.");
            return this;
        }

        /** Replaces the ordered conjunction of filter hints. */
        public Builder filters(List<Filter> filters) {
            Objects.requireNonNull(filters, "Filters cannot be null.");
            List<Filter> copy = new ArrayList<>(filters.size());
            for (Filter filter : filters) {
                copy.add(Objects.requireNonNull(filter, "A filter cannot be null."));
            }
            this.filters = List.copyOf(copy);
            return this;
        }

        /** Limits returned rows after any requested filtering. */
        public Builder limit(long limit) {
            if (limit < 0) {
                throw new IllegalArgumentException("A read limit cannot be negative.");
            }
            this.limit = limit;
            return this;
        }

        /** Builds an immutable read operation. */
        public ReadRequest build() {
            return new ReadRequest(this);
        }
    }
}
