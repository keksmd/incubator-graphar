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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.graphar.reader.EdgeLayoutReader;
import org.apache.graphar.writer.EdgeRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Opt-in volume run. Set {@code GRAPHAR_SCALE_VERTICES}, {@code GRAPHAR_SCALE_EDGES}, {@code
 * GRAPHAR_SCALE_PATCHES}, and {@code GRAPHAR_SCALE_PARTITIONS} to size the dataset.
 *
 * <p>Chunk-scoped compaction exists because folding patches back in by rebuilding the whole layout
 * costs a rewrite of everything, most of which nothing changed in. The unit tests state that the
 * rewrite is correct and touches one chunk; they say nothing about what that is worth. This builds
 * a layout of a size the decision is actually made at, folds a hot chunk, and reports the bytes and
 * the time against a full rebuild over the same union. It asserts only that the portioned rewrite
 * stays below the full one, because throughput depends on the host.
 */
public class ChunkCompactionScaleIT {
    private static final long DEFAULT_VERTICES = 200_000L;
    private static final long DEFAULT_EDGES = 2_000_000L;
    private static final long DEFAULT_PATCHES = 50_000L;
    private static final long DEFAULT_PARTITIONS = 64L;
    private static final long EDGE_CHUNK_SIZE = 65_536L;
    private static final long SEED = 20260821L;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void foldingOneChunkCostsLessThanRebuildingTheLayout() throws Exception {
        long vertexCount = size("GRAPHAR_SCALE_VERTICES", DEFAULT_VERTICES);
        long edgeCount = size("GRAPHAR_SCALE_EDGES", DEFAULT_EDGES);
        long patchCount = size("GRAPHAR_SCALE_PATCHES", DEFAULT_PATCHES);
        long partitionCount = size("GRAPHAR_SCALE_PARTITIONS", DEFAULT_PARTITIONS);
        long vertexChunkSize = (vertexCount + partitionCount - 1) / partitionCount;
        long hotPartition = partitionCount / 2;

        CompactionDataset base =
                new CompactionDataset(
                        temporaryFolder.newFolder("base").toPath(),
                        vertexChunkSize,
                        EDGE_CHUNK_SIZE);
        base.writeLayout(vertexCount, edges(edgeCount, 0L, vertexCount, vertexCount));
        long baseBytes = bytes(base.rootPath());
        List<EdgeRecord> patches =
                materialize(
                        edges(
                                patchCount,
                                hotPartition * vertexChunkSize,
                                Math.min(vertexCount, (hotPartition + 1) * vertexChunkSize),
                                vertexCount));

        InMemoryPatchSource source = new InMemoryPatchSource();
        for (EdgeRecord patch : patches) {
            source.add(base.chunk(hotPartition), patch);
        }
        long compactStart = System.nanoTime();
        CompactedChunk compacted =
                new ChunkCompactor(base.reader(), base.writer(), base.storage())
                        .compact(base.chunk(hotPartition), source);
        long compactMillis = (System.nanoTime() - compactStart) / 1_000_000L;

        CompactionDataset rebuilt =
                new CompactionDataset(
                        temporaryFolder.newFolder("rebuilt").toPath(),
                        vertexChunkSize,
                        EDGE_CHUNK_SIZE);
        long rebuildStart = System.nanoTime();
        rebuilt.writeLayout(
                vertexCount, concat(edges(edgeCount, 0L, vertexCount, vertexCount), patches));
        long rebuildMillis = (System.nanoTime() - rebuildStart) / 1_000_000L;

        System.out.println(
                "scale: vertices="
                        + vertexCount
                        + " edges="
                        + edgeCount
                        + " patches="
                        + patchCount
                        + " partitions="
                        + partitionCount
                        + " baseBytes="
                        + baseBytes
                        + " compactedChunkEdges="
                        + compacted.edgeCount()
                        + " compactedBytes="
                        + compacted.bytesRewritten()
                        + " compactMillis="
                        + compactMillis
                        + " rebuildBytes="
                        + bytes(rebuilt.rootPath())
                        + " rebuildMillis="
                        + rebuildMillis);

        assertEquals(
                "a folded chunk must hold what a rebuild puts in it",
                partitionEdgeCounts(rebuilt),
                partitionEdgeCounts(base));
        assertEquals(edgeCount + patchCount, totalEdgeCount(base));
        assertTrue(
                "folding one chunk must rewrite less than the whole layout: "
                        + compacted.bytesRewritten()
                        + " of "
                        + baseBytes,
                compacted.bytesRewritten() < baseBytes);
        assertTrue(
                "folding one chunk must take less than rebuilding the layout: "
                        + compactMillis
                        + "ms against "
                        + rebuildMillis
                        + "ms",
                compactMillis < rebuildMillis);
    }

    private static List<Long> partitionEdgeCounts(CompactionDataset dataset) throws IOException {
        EdgeLayoutReader layout =
                dataset.reader()
                        .edge(
                                CompactionDataset.SRC_TYPE,
                                CompactionDataset.EDGE_TYPE,
                                CompactionDataset.DST_TYPE,
                                CompactionDataset.LAYOUT);
        List<Long> counts = new ArrayList<>();
        for (long partition = 0; partition < layout.partitionCount(); partition++) {
            counts.add(layout.partitionEdgeCount(partition));
        }
        return counts;
    }

    private static long totalEdgeCount(CompactionDataset dataset) throws IOException {
        return dataset.reader()
                .edge(
                        CompactionDataset.SRC_TYPE,
                        CompactionDataset.EDGE_TYPE,
                        CompactionDataset.DST_TYPE,
                        CompactionDataset.LAYOUT)
                .edgeCount();
    }

    private static List<EdgeRecord> materialize(Iterable<EdgeRecord> records) {
        List<EdgeRecord> values = new ArrayList<>();
        for (EdgeRecord record : records) {
            values.add(record);
        }
        return values;
    }

    /**
     * Returns a re-iterable synthetic edge source. The records are generated on demand from a fixed
     * seed rather than held in a list, so the volume the writer streams is not also the volume the
     * test holds in heap.
     */
    private static Iterable<EdgeRecord> edges(
            long count, long sourceFrom, long sourceUntil, long vertexCount) {
        long span = Math.max(1L, sourceUntil - sourceFrom);
        return () ->
                new Iterator<EdgeRecord>() {
                    private final Random random = new Random(SEED + sourceFrom);
                    private long produced;

                    @Override
                    public boolean hasNext() {
                        return produced < count;
                    }

                    @Override
                    public EdgeRecord next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException("No synthetic edge left.");
                        }
                        produced++;
                        long source = sourceFrom + Math.floorMod(random.nextLong(), span);
                        long destination = Math.floorMod(random.nextLong(), vertexCount);
                        return CompactionDataset.edge(source, destination);
                    }
                };
    }

    private static Iterable<EdgeRecord> concat(
            Iterable<EdgeRecord> first, Iterable<EdgeRecord> second) {
        return () ->
                new Iterator<EdgeRecord>() {
                    private final Iterator<EdgeRecord> head = first.iterator();
                    private final Iterator<EdgeRecord> tail = second.iterator();

                    @Override
                    public boolean hasNext() {
                        return head.hasNext() || tail.hasNext();
                    }

                    @Override
                    public EdgeRecord next() {
                        return head.hasNext() ? head.next() : tail.next();
                    }
                };
    }

    private static long bytes(Path root) throws IOException {
        long total = 0L;
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : (Iterable<Path>) paths.filter(Files::isRegularFile)::iterator) {
                total += Files.size(path);
            }
        }
        return total;
    }

    private static long size(String variable, long fallback) {
        String configured = System.getenv(variable);
        return configured == null ? fallback : Long.parseLong(configured);
    }
}
