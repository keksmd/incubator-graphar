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

/** A half-open range of non-negative chunk indexes. */
public final class ChunkRange {
    private final long begin;
    private final long end;

    /** Creates a half-open chunk range. Begin must be non-negative and not exceed end. */
    public ChunkRange(long begin, long end) {
        if (begin < 0) {
            throw new IllegalArgumentException("Chunk range begin must be non-negative: " + begin);
        }
        if (end < begin) {
            throw new IllegalArgumentException(
                    "Chunk range end must not precede begin: [" + begin + ", " + end + ")");
        }
        this.begin = begin;
        this.end = end;
    }

    /** Returns the first included chunk index. */
    public long begin() {
        return begin;
    }

    /** Returns the first excluded chunk index. */
    public long end() {
        return end;
    }

    /** Returns whether this range selects no chunks. */
    public boolean isEmpty() {
        return begin == end;
    }

    /** Returns whether {@code chunkIndex} belongs to this range. */
    public boolean contains(long chunkIndex) {
        return chunkIndex >= begin && chunkIndex < end;
    }
}
