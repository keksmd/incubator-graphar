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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.graphar.reader.CsrDirection;
import org.apache.graphar.reader.EdgePropertyCursor;
import org.apache.graphar.reader.GraphProjection;
import org.apache.graphar.reader.GraphReader;
import org.apache.graphar.reader.HeterogeneousCsr;
import org.apache.graphar.writer.EdgeRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Pins how a compaction interacts with the projection served from the dataset it rewrites: nothing
 * observes the dataset while it is part old chunk and part new one, and the patches that arrive
 * while it runs are still there afterwards.
 */
public class ChunkCompactionPublishTest {
    private static final long VERTEX_COUNT = 12L;
    private static final long VERTEX_CHUNK_SIZE = 3L;
    private static final long EDGE_CHUNK_SIZE = 2L;
    private static final long HOT_PARTITION = 1L;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void aRebuildStartedDuringACompactionKeepsServingTheProjectionBeforeIt()
            throws Exception {
        CompactionDataset dataset = dataset("fenced");
        dataset.writeVertices(VERTEX_COUNT);
        dataset.writeLayout(VERTEX_COUNT, baseEdges());
        GraphProjection projection = GraphProjection.load(() -> build(dataset));
        GraphProjection.Snapshot serving = projection.current();
        assertEquals(baseEdges().size(), serving.projection().edgeCount());

        InMemoryPatchSource source = patchSource(dataset, patches());
        List<GraphProjection.Snapshot> observed = new ArrayList<>();
        CompactedChunk compacted =
                projection.rewriteDataset(
                        () -> {
                            CompactedChunk result =
                                    compactor(dataset)
                                            .compact(dataset.chunk(HOT_PARTITION), source);
                            observed.add(projection.refresh());
                            observed.add(projection.current());
                            return result;
                        });

        assertEquals(patches().size(), compacted.patchEdgeCount());
        for (GraphProjection.Snapshot snapshot : observed) {
            assertSame("a rebuild must not run over a dataset being rewritten", serving, snapshot);
        }
        assertSame(serving, projection.current());
        GraphProjection.Snapshot republished = projection.refresh();
        assertEquals(2L, republished.generation());
        assertEquals(baseEdges().size() + patches().size(), republished.projection().edgeCount());
    }

    @Test
    public void aCompactionIsRefusedWhileARebuildIsInFlight() throws Exception {
        CompactionDataset dataset = dataset("in-flight");
        dataset.writeVertices(VERTEX_COUNT);
        dataset.writeLayout(VERTEX_COUNT, baseEdges());
        AtomicReference<GraphProjection> holder = new AtomicReference<>();
        AtomicReference<RuntimeException> refused = new AtomicReference<>();
        GraphProjection projection =
                GraphProjection.load(
                        () -> {
                            GraphProjection current = holder.get();
                            if (current != null) {
                                refused.set(
                                        assertThrows(
                                                IllegalStateException.class,
                                                () -> current.rewriteDataset(() -> null)));
                            }
                            return build(dataset);
                        });
        holder.set(projection);

        projection.refresh();

        assertNotNull("a rebuild must refuse to share the dataset with a rewrite", refused.get());
    }

    @Test
    public void patchesThatArriveDuringACompactionSurviveItAndFoldInOnTheNextOne()
            throws Exception {
        CompactionDataset dataset = dataset("late");
        dataset.writeLayout(VERTEX_COUNT, baseEdges());
        EdgeRecord late = CompactionDataset.edge(4L, 11L);
        InMemoryPatchSource source = patchSource(dataset, patches());
        source.afterRead(() -> source.add(dataset.chunk(HOT_PARTITION), late));

        CompactedChunk first = compactor(dataset).compact(dataset.chunk(HOT_PARTITION), source);

        assertEquals(patches().size(), first.patchEdgeCount());
        assertEquals(1, source.pendingFor(dataset.chunk(HOT_PARTITION)).size());
        assertEquals(baseEdges().size() + patches().size(), edgeCount(dataset.reader()));

        source.afterRead(() -> {});
        CompactedChunk second = compactor(dataset).compact(dataset.chunk(HOT_PARTITION), source);

        assertEquals(1L, second.patchEdgeCount());
        assertTrue(second.watermark() > first.watermark());
        assertEquals(List.of(), source.pendingFor(dataset.chunk(HOT_PARTITION)));

        CompactionDataset rebuilt = dataset("late-rebuilt");
        List<EdgeRecord> union = new ArrayList<>(baseEdges());
        union.addAll(patches());
        union.add(late);
        rebuilt.writeLayout(VERTEX_COUNT, union);
        assertEquals(rows(rebuilt.reader()), rows(dataset.reader()));
    }

    private CompactionDataset dataset(String name) throws IOException {
        return new CompactionDataset(
                temporaryFolder.newFolder(name).toPath(), VERTEX_CHUNK_SIZE, EDGE_CHUNK_SIZE);
    }

    private static ChunkCompactor compactor(CompactionDataset dataset) {
        return new ChunkCompactor(dataset.reader(), dataset.writer(), dataset.storage());
    }

    private static InMemoryPatchSource patchSource(
            CompactionDataset dataset, List<EdgeRecord> records) {
        InMemoryPatchSource source = new InMemoryPatchSource();
        for (EdgeRecord record : records) {
            source.add(dataset.chunk(HOT_PARTITION), record);
        }
        return source;
    }

    private static HeterogeneousCsr build(CompactionDataset dataset) throws IOException {
        return HeterogeneousCsr.builder(dataset.reader())
                .addVertexType(CompactionDataset.SRC_TYPE)
                .addEdgeType(
                        CompactionDataset.SRC_TYPE,
                        CompactionDataset.EDGE_TYPE,
                        CompactionDataset.DST_TYPE)
                .direction(CsrDirection.OUTGOING)
                .build();
    }

    private static long edgeCount(GraphReader reader) throws IOException {
        return reader.edge(
                        CompactionDataset.SRC_TYPE,
                        CompactionDataset.EDGE_TYPE,
                        CompactionDataset.DST_TYPE,
                        CompactionDataset.LAYOUT)
                .edgeCount();
    }

    private static List<String> rows(GraphReader reader) throws IOException {
        List<String> values = new ArrayList<>();
        try (EdgePropertyCursor cursor =
                reader.edge(
                                CompactionDataset.SRC_TYPE,
                                CompactionDataset.EDGE_TYPE,
                                CompactionDataset.DST_TYPE,
                                CompactionDataset.LAYOUT)
                        .scanEdges()) {
            while (cursor.next()) {
                values.add(
                        cursor.edge().source()
                                + ">"
                                + cursor.edge().destination()
                                + "@"
                                + cursor.edge().properties().get(CompactionDataset.WEIGHT));
            }
        }
        return values;
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
}
