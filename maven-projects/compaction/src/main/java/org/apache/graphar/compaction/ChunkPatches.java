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

package org.apache.graphar.compaction;

import java.util.List;
import java.util.Objects;
import org.apache.graphar.writer.EdgeRecord;

/**
 * The patches one chunk had accumulated at the moment they were read out of the Delta, together
 * with the position that read reached.
 *
 * <p>The records are the edges to fold into the base partition, in the order the Delta accumulated
 * them. A compaction has to preserve that order relative to the base rows, because it is what makes
 * the rewritten partition equal to a rebuild of the layout over the base input followed by the
 * patches.
 *
 * <p>The watermark is opaque to compaction. It is the source's own answer to "which patches are
 * these", and it exists so the slice that is dropped afterwards is exactly the slice that was
 * folded in: patches that arrive for the same chunk while the rewrite runs are past the watermark
 * and survive it.
 */
public final class ChunkPatches {
    private final EdgeChunk chunk;
    private final List<EdgeRecord> records;
    private final long watermark;

    /** Creates the patch slice read for one chunk up to {@code watermark}. */
    public ChunkPatches(EdgeChunk chunk, List<EdgeRecord> records, long watermark) {
        this.chunk = Objects.requireNonNull(chunk, "Edge chunk cannot be null.");
        this.records =
                List.copyOf(Objects.requireNonNull(records, "Patch records cannot be null."));
        for (EdgeRecord record : this.records) {
            Objects.requireNonNull(record, "Patch record cannot be null.");
        }
        if (watermark < 0) {
            throw new IllegalArgumentException("Patch watermark cannot be negative: " + watermark);
        }
        this.watermark = watermark;
    }

    /** Returns an empty slice standing at {@code watermark}. */
    public static ChunkPatches empty(EdgeChunk chunk, long watermark) {
        return new ChunkPatches(chunk, List.of(), watermark);
    }

    /** Returns the chunk these patches belong to. */
    public EdgeChunk chunk() {
        return chunk;
    }

    /** Returns the patch edges in the order the Delta accumulated them. */
    public List<EdgeRecord> records() {
        return records;
    }

    /** Returns the source position this slice was read up to. */
    public long watermark() {
        return watermark;
    }

    /** Reports whether this chunk had nothing pending. */
    public boolean isEmpty() {
        return records.isEmpty();
    }
}
