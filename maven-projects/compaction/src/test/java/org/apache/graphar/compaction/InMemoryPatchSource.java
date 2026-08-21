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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.graphar.writer.EdgeRecord;

/**
 * A Delta that holds pending patches in memory, ordered by arrival, and drops a slice by the
 * watermark a compaction folded in.
 *
 * <p>The watermark counts every patch this source has ever accepted for a chunk, so it names the
 * position a read reached rather than a number of records left over, which is what lets patches
 * that arrive during a compaction survive it.
 */
final class InMemoryPatchSource implements ChunkPatchSource {
    private final Map<EdgeChunk, Deque<EdgeRecord>> pending = new HashMap<>();
    private final Map<EdgeChunk, Long> dropped = new HashMap<>();
    private final List<CompactedChunk> drops = new ArrayList<>();
    private Runnable afterRead = () -> {};

    void add(EdgeChunk chunk, EdgeRecord record) {
        pending.computeIfAbsent(chunk, key -> new ArrayDeque<>()).addLast(record);
    }

    /** Runs after every read, which is how a patch arriving mid-compaction is simulated. */
    void afterRead(Runnable action) {
        this.afterRead = action;
    }

    List<EdgeRecord> pendingFor(EdgeChunk chunk) {
        return new ArrayList<>(pending.getOrDefault(chunk, new ArrayDeque<>()));
    }

    List<CompactedChunk> drops() {
        return drops;
    }

    @Override
    public ChunkPatches patchesFor(EdgeChunk chunk) throws IOException {
        List<EdgeRecord> records = pendingFor(chunk);
        long watermark = dropped.getOrDefault(chunk, 0L) + records.size();
        ChunkPatches patches = new ChunkPatches(chunk, records, watermark);
        afterRead.run();
        return patches;
    }

    @Override
    public void drop(CompactedChunk compacted) throws IOException {
        drops.add(compacted);
        EdgeChunk chunk = compacted.chunk();
        long alreadyDropped = dropped.getOrDefault(chunk, 0L);
        long remove = compacted.watermark() - alreadyDropped;
        Deque<EdgeRecord> queue = pending.getOrDefault(chunk, new ArrayDeque<>());
        for (long index = 0; index < remove && !queue.isEmpty(); index++) {
            queue.removeFirst();
        }
        dropped.put(chunk, Math.max(alreadyDropped, compacted.watermark()));
    }
}
