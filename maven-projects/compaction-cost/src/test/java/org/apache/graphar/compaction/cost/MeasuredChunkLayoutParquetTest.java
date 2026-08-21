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

package org.apache.graphar.compaction.cost;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.graphar.info.AdjacentList;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.storage.local.LocalStorage;
import org.apache.graphar.writer.GraphWriter;
import org.apache.graphar.writer.TopologyEdge;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Measures a GraphAr Parquet dataset that was actually written, and states the compaction estimate
 * in the bytes that dataset occupies.
 *
 * <p>Sizes decide whether a rewrite is worth doing, so an estimate priced against invented sizes
 * decides nothing. This test closes that loop once: the layout is read off the files the writer
 * produced, and the estimate is checked against those same files measured a second way.
 */
public class MeasuredChunkLayoutParquetTest {
    private static final AdjListType ORDERED_BY_SOURCE = AdjListType.ordered_by_source;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void aRewriteIsPricedInTheBytesTheDatasetActuallyOccupies() throws Exception {
        Path rootPath = temporaryFolder.newFolder("measured").toPath();
        URI root = rootPath.toUri();
        EdgeInfo edgeInfo = write(root);

        MeasuredChunkLayout layout = measure(root, edgeInfo, 8, new long[] {6, 1, 2, 0});

        assertEquals(4, layout.chunkCount());
        assertEquals(2, layout.vertexChunkSize());
        for (long chunkIndex = 0; chunkIndex < 4; chunkIndex++) {
            assertEquals(bytesUnder(rootPath, chunkIndex), layout.baseBytes(chunkIndex));
        }
        assertTrue(layout.baseBytes(0) > layout.baseBytes(3));

        DeltaCostLedger ledger = new DeltaCostLedger(2);
        ledger.patched(0, 3, 512);
        ledger.patched(5, 1, 128);
        CompactionEstimator estimator = new CompactionEstimator(layout, ledger.snapshot());

        CompactionEstimate hottest = estimator.estimate(0);
        assertEquals(bytesUnder(rootPath, 0), hottest.baseBytesTouched());
        assertEquals(6, hottest.baseEdgesTouched());
        assertEquals(512, hottest.deltaBytesRemovedIfCompacted());
        assertEquals(
                hottest.baseBytesTouched() + Math.round(3 * (hottest.baseBytesTouched() / 6.0d)),
                hottest.estimatedRewriteBytes());
        assertEquals(
                hottest.baseBytesTouched() / 512.0d, hottest.compactionAmplification(), 1.0e-9d);

        CompactionEstimate whole = estimator.estimateAll();
        assertEquals(2, whole.rewrittenChunks());
        assertEquals(bytesUnder(rootPath, 0) + bytesUnder(rootPath, 2), whole.baseBytesTouched());
        assertEquals(640, whole.deltaBytesRemovedIfCompacted());
        assertTrue(whole.estimatedRewriteBytes() > whole.baseBytesTouched());
    }

    private static EdgeInfo write(URI root) throws IOException {
        PropertyGroup vertexGroup =
                new PropertyGroup(
                        List.of(new Property("id", DataType.INT64, true, false)),
                        FileType.PARQUET,
                        "id/");
        VertexInfo vertex =
                new VertexInfo(
                        "person", 2, List.of(vertexGroup), URI.create("vertex/person/"), "gar/v1");
        EdgeInfo edge =
                new EdgeInfo(
                        "person",
                        "knows",
                        "person",
                        2,
                        2,
                        2,
                        true,
                        URI.create("edge/person_knows_person/"),
                        "gar/v1",
                        List.of(
                                new AdjacentList(
                                        ORDERED_BY_SOURCE, FileType.PARQUET, "ordered_by_source/")),
                        List.of());
        GraphInfo graph =
                new GraphInfo(
                        "tiny",
                        Map.of(URI.create("person.vertex.yml"), vertex),
                        Map.of(URI.create("person_knows_person.edge.yml"), edge),
                        root,
                        "gar/v1");
        LocalStorage storage = new LocalStorage();
        GraphWriter writer =
                new GraphWriter(
                        graph,
                        root,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        WriteMode.OVERWRITE);
        writer.writeOrderedSourceTopology(
                edge,
                8,
                List.of(
                        new TopologyEdge(0, 1),
                        new TopologyEdge(0, 2),
                        new TopologyEdge(0, 3),
                        new TopologyEdge(0, 4),
                        new TopologyEdge(0, 5),
                        new TopologyEdge(1, 2),
                        new TopologyEdge(2, 3),
                        new TopologyEdge(4, 5),
                        new TopologyEdge(4, 6)));
        return edge;
    }

    private static MeasuredChunkLayout measure(
            URI root, EdgeInfo edgeInfo, long vertexCount, long[] edgesPerChunk)
            throws IOException {
        MeasuredChunkLayout.Builder builder = MeasuredChunkLayout.builder(2, vertexCount);
        for (long chunkIndex = 0; chunkIndex < edgesPerChunk.length; chunkIndex++) {
            long bytes = sizeOf(root, edgeInfo.getOffsetChunkUri(ORDERED_BY_SOURCE, chunkIndex));
            for (long edgeChunkIndex = 0; ; edgeChunkIndex++) {
                long chunkBytes =
                        sizeOf(
                                root,
                                edgeInfo.getAdjacentListChunkUri(
                                        ORDERED_BY_SOURCE, chunkIndex, edgeChunkIndex));
                if (chunkBytes == 0) {
                    break;
                }
                bytes += chunkBytes;
            }
            builder.chunk(chunkIndex, bytes, edgesPerChunk[(int) chunkIndex]);
        }
        return builder.build();
    }

    private static long sizeOf(URI root, URI relative) throws IOException {
        Path path = Paths.get(root.resolve(relative));
        return Files.isRegularFile(path) ? Files.size(path) : 0L;
    }

    private static long bytesUnder(Path rootPath, long chunkIndex) throws IOException {
        Path base = rootPath.resolve("edge/person_knows_person/ordered_by_source");
        List<Path> roots = new ArrayList<>();
        roots.add(base.resolve("adj_list/part" + chunkIndex));
        roots.add(base.resolve("offset/chunk" + chunkIndex));
        long bytes = 0;
        for (Path candidate : roots) {
            if (!Files.exists(candidate)) {
                continue;
            }
            try (Stream<Path> walk = Files.walk(candidate)) {
                for (Path path : (Iterable<Path>) walk::iterator) {
                    if (Files.isRegularFile(path)) {
                        bytes += Files.size(path);
                    }
                }
            }
        }
        return bytes;
    }
}
