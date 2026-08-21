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

import java.net.URI;
import java.util.Objects;

/** The offset chunk and pair index required to resolve one ordered-layout vertex. */
public final class OffsetLocation {
    private final long vertexId;
    private final long vertexChunkIndex;
    private final long offsetIndex;
    private final URI offsetChunkUri;

    OffsetLocation(long vertexId, long vertexChunkIndex, long offsetIndex, URI offsetChunkUri) {
        this.vertexId = vertexId;
        this.vertexChunkIndex = vertexChunkIndex;
        this.offsetIndex = offsetIndex;
        this.offsetChunkUri =
                Objects.requireNonNull(offsetChunkUri, "Offset chunk URI cannot be null.");
    }

    /** Returns the vertex this location addresses. */
    public long vertexId() {
        return vertexId;
    }

    /** Returns the vertex chunk that owns the vertex. */
    public long vertexChunkIndex() {
        return vertexChunkIndex;
    }

    /** Returns the first of the two adjacent offset values to read. */
    public long offsetIndex() {
        return offsetIndex;
    }

    /** Returns the offset chunk holding the two adjacent values. */
    public URI offsetChunkUri() {
        return offsetChunkUri;
    }
}
