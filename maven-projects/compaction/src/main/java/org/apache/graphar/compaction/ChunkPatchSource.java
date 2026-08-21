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

import java.io.IOException;

/**
 * The mutable Delta as chunk-scoped compaction needs to see it: patches readable per chunk, and a
 * slice droppable by the position it was read to.
 *
 * <p>Compaction owns no Delta of its own. It is defined against this interface so that the base
 * rewrite and the structure that holds pending patches stay separable, and so that a compaction can
 * be measured against a source that only holds patches in memory.
 */
public interface ChunkPatchSource {
    /**
     * Reads everything pending for one chunk and reports the position that read reached. A source
     * that keeps accepting patches during a compaction must return a watermark that excludes what
     * arrives after this call.
     */
    ChunkPatches patchesFor(EdgeChunk chunk) throws IOException;

    /**
     * Drops exactly the patches for {@code compacted.chunk()} up to {@code compacted.watermark()},
     * keeping everything that arrived after it.
     *
     * <p>This is called after the base partition holding those patches has been published, so a
     * failure between the two leaves them pending and a repeated compaction folds them in twice.
     * The compactor is therefore at-least-once, and a source that cannot tolerate that has {@link
     * CompactedChunk#baseEdgeCount()} and {@link CompactedChunk#edgeCount()} to recognize a rewrite
     * that already landed. Dropping an already-dropped slice must be a no-op.
     */
    void drop(CompactedChunk compacted) throws IOException;
}
