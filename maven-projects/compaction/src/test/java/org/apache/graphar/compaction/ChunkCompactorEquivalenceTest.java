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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.reader.EdgeLayoutReader;
import org.apache.graphar.reader.EdgePropertyCursor;
import org.apache.graphar.reader.GraphEdge;
import org.apache.graphar.reader.GraphReader;
import org.apache.graphar.writer.EdgeRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * States the correctness of chunk-scoped compaction as an equivalence: folding the patches of one
 * chunk into that chunk produces the dataset a full rebuild over the base edges followed by the
 * patches produces, and leaves every file outside that chunk exactly as it was.
 */
public class ChunkCompactorEquivalenceTest {
    private static final long VERTEX_COUNT = 12L;
    private static final long VERTEX_CHUNK_SIZE = 3L;
    private static final long EDGE_CHUNK_SIZE = 2L;
    private static final long HOT_PARTITION = 1L;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void aCompactedChunkEqualsARebuildOverTheBaseFollowedByThePatches() throws Exception {
        CompactionDataset base = dataset("base");
        base.writeLayout(VERTEX_COUNT, baseEdges());
        CompactionDataset rebuilt = dataset("rebuilt");
        List<EdgeRecord> union = new ArrayList<>(baseEdges());
        union.addAll(patches());
        rebuilt.writeLayout(VERTEX_COUNT, union);

        InMemoryPatchSource source = new InMemoryPatchSource();
        for (EdgeRecord patch : patches()) {
            source.add(base.chunk(HOT_PARTITION), patch);
        }
        CompactedChunk compacted =
                new ChunkCompactor(base.reader(), base.writer(), base.storage())
                        .compact(base.chunk(HOT_PARTITION), source);

        assertEquals(4L, compacted.baseEdgeCount());
        assertEquals(patches().size(), compacted.patchEdgeCount());
        assertEquals(4L + patches().size(), compacted.edgeCount());
        assertTrue(compacted.bytesRewritten() > 0L);
        assertEquals(rows(rebuilt.reader()), rows(base.reader()));
        assertEquals(edgeCounts(rebuilt.reader()), edgeCounts(base.reader()));
        for (long vertex = 0; vertex < VERTEX_COUNT; vertex++) {
            assertEquals(
                    "vertex " + vertex + " must expand to what a rebuild would expand it to",
                    neighbors(rebuilt.reader(), vertex),
                    neighbors(base.reader(), vertex));
        }
        assertEquals(List.of(compacted), source.drops());
        assertEquals(List.of(), source.pendingFor(base.chunk(HOT_PARTITION)));
    }

    @Test
    public void compactingOneChunkLeavesEveryOtherChunkByteIdentical() throws Exception {
        CompactionDataset base = dataset("scoped");
        base.writeLayout(VERTEX_COUNT, baseEdges());
        Map<String, String> before = digests(base.rootPath());

        InMemoryPatchSource source = new InMemoryPatchSource();
        for (EdgeRecord patch : patches()) {
            source.add(base.chunk(HOT_PARTITION), patch);
        }
        new ChunkCompactor(base.reader(), base.writer(), base.storage())
                .compact(base.chunk(HOT_PARTITION), source);

        Map<String, String> after = digests(base.rootPath());
        List<String> changed = new ArrayList<>();
        for (Map.Entry<String, String> entry : after.entrySet()) {
            if (!entry.getValue().equals(before.get(entry.getKey()))) {
                changed.add(entry.getKey());
            }
        }
        assertTrue("a compaction must rewrite something: " + changed, !changed.isEmpty());
        for (String path : changed) {
            assertTrue(
                    "only the compacted chunk may change: " + path,
                    path.contains("/part" + HOT_PARTITION + "/")
                            || path.endsWith("offset/chunk" + HOT_PARTITION)
                            || path.endsWith("edge_count" + HOT_PARTITION));
        }
        assertTrue(
                "a compaction must not remove a file the layout still needs",
                after.keySet().containsAll(before.keySet()));
        assertEquals(
                before.get("edge/person_knows_person/ordered_by_source/vertex_count"),
                after.get("edge/person_knows_person/ordered_by_source/vertex_count"));
        assertNotEquals(
                before.get("edge/person_knows_person/ordered_by_source/edge_count1"),
                after.get("edge/person_knows_person/ordered_by_source/edge_count1"));
    }

    @Test
    public void aChunkWithNothingPendingIsLeftAlone() throws Exception {
        CompactionDataset base = dataset("cold");
        base.writeLayout(VERTEX_COUNT, baseEdges());
        Map<String, String> before = digests(base.rootPath());

        InMemoryPatchSource source = new InMemoryPatchSource();
        CompactedChunk compacted =
                new ChunkCompactor(base.reader(), base.writer(), base.storage())
                        .compact(base.chunk(HOT_PARTITION), source);

        assertEquals(0L, compacted.patchEdgeCount());
        assertEquals(compacted.baseEdgeCount(), compacted.edgeCount());
        assertEquals(0L, compacted.bytesRewritten());
        assertEquals(List.of(), source.drops());
        assertEquals(before, digests(base.rootPath()));
    }

    @Test
    public void aPatchAddressedToAnotherChunkIsRefused() throws Exception {
        CompactionDataset base = dataset("misrouted");
        base.writeLayout(VERTEX_COUNT, baseEdges());

        InMemoryPatchSource source = new InMemoryPatchSource();
        source.add(base.chunk(HOT_PARTITION), CompactionDataset.edge(0L, 11L));
        ChunkCompactor compactor = new ChunkCompactor(base.reader(), base.writer(), base.storage());

        assertThrows(
                IllegalArgumentException.class,
                () -> compactor.compact(base.chunk(HOT_PARTITION), source));
    }

    @Test
    public void aChunkOutsideTheLayoutIsRefused() throws Exception {
        CompactionDataset base = dataset("outside");
        base.writeLayout(VERTEX_COUNT, baseEdges());
        ChunkCompactor compactor = new ChunkCompactor(base.reader(), base.writer(), base.storage());

        assertThrows(
                IllegalArgumentException.class,
                () -> compactor.compact(base.chunk(9L), new InMemoryPatchSource()));
    }

    @Test
    public void aWriterThatCannotOverwriteIsRefused() throws Exception {
        CompactionDataset base = dataset("create-new");
        base.writeLayout(VERTEX_COUNT, baseEdges());
        InMemoryPatchSource source = new InMemoryPatchSource();
        source.add(base.chunk(HOT_PARTITION), CompactionDataset.edge(3L, 11L));
        ChunkCompactor compactor =
                new ChunkCompactor(
                        base.reader(), base.writer(WriteMode.CREATE_NEW), base.storage());

        assertThrows(
                IllegalStateException.class,
                () -> compactor.compact(base.chunk(HOT_PARTITION), source));
    }

    private CompactionDataset dataset(String name) throws IOException {
        return new CompactionDataset(
                temporaryFolder.newFolder(name).toPath(), VERTEX_CHUNK_SIZE, EDGE_CHUNK_SIZE);
    }

    private static List<EdgeRecord> baseEdges() {
        return List.of(
                CompactionDataset.edge(0L, 1L),
                CompactionDataset.edge(0L, 5L),
                CompactionDataset.edge(1L, 2L),
                CompactionDataset.edge(2L, 7L),
                CompactionDataset.edge(3L, 4L),
                CompactionDataset.edge(3L, 9L),
                CompactionDataset.edge(4L, 0L),
                CompactionDataset.edge(5L, 6L),
                CompactionDataset.edge(6L, 1L),
                CompactionDataset.edge(7L, 8L),
                CompactionDataset.edge(9L, 10L),
                CompactionDataset.edge(10L, 11L),
                CompactionDataset.edge(11L, 0L));
    }

    private static List<EdgeRecord> patches() {
        return List.of(
                CompactionDataset.edge(4L, 7L),
                CompactionDataset.edge(3L, 0L),
                CompactionDataset.edge(5L, 5L),
                CompactionDataset.edge(3L, 4L));
    }

    private static List<String> rows(GraphReader reader) throws IOException {
        List<String> values = new ArrayList<>();
        try (EdgePropertyCursor cursor = layout(reader).scanEdges()) {
            while (cursor.next()) {
                values.add(format(cursor.edge()));
            }
        }
        return values;
    }

    private static List<String> neighbors(GraphReader reader, long vertex) throws IOException {
        List<String> values = new ArrayList<>();
        try (EdgePropertyCursor cursor = layout(reader).edges(vertex)) {
            while (cursor.next()) {
                values.add(format(cursor.edge()));
            }
        }
        return values;
    }

    private static List<Long> edgeCounts(GraphReader reader) throws IOException {
        EdgeLayoutReader layout = layout(reader);
        List<Long> counts = new ArrayList<>();
        for (long partition = 0; partition < layout.partitionCount(); partition++) {
            counts.add(layout.partitionEdgeCount(partition));
        }
        return counts;
    }

    private static EdgeLayoutReader layout(GraphReader reader) {
        return reader.edge(
                CompactionDataset.SRC_TYPE,
                CompactionDataset.EDGE_TYPE,
                CompactionDataset.DST_TYPE,
                CompactionDataset.LAYOUT);
    }

    private static String format(GraphEdge edge) {
        return edge.source()
                + ">"
                + edge.destination()
                + "@"
                + edge.properties().get(CompactionDataset.WEIGHT);
    }

    private static Map<String, String> digests(Path root) throws IOException {
        Map<String, String> digests = new TreeMap<>();
        try (java.util.stream.Stream<Path> paths = java.nio.file.Files.walk(root)) {
            for (Path path :
                    (Iterable<Path>) paths.filter(java.nio.file.Files::isRegularFile)::iterator) {
                digests.put(
                        root.relativize(path).toString().replace('\\', '/'),
                        digest(java.nio.file.Files.readAllBytes(path)));
            }
        }
        return digests;
    }

    private static String digest(byte[] bytes) {
        try {
            byte[] hash = java.security.MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder text = new StringBuilder(hash.length * 2);
            for (byte value : hash) {
                text.append(String.format("%02x", value));
            }
            return text.toString();
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new AssertionError(failure);
        }
    }
}
