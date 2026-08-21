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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.info.type.AdjListType;
import org.junit.Test;

public class ArrivalClassifierFixtureTest {
    private static final Path FIXTURE_ROOT =
            Path.of("..", "..", "testing", "ldbc_sample", "parquet");
    private static final long MATERIALIZED_PERSONS = 903;

    private static final long[] SOURCES = {297, MATERIALIZED_PERSONS, 7, MATERIALIZED_PERSONS + 3};
    private static final long[] TARGETS = {
        10, 0, MATERIALIZED_PERSONS + 1, MATERIALIZED_PERSONS + 4
    };

    @Test
    public void splitsAnLdbcBatchAgainstTheMaterializedSourceFrontier() throws Exception {
        EdgeInfo edgeInfo = loadPersonKnowsPerson();
        assertEquals(
                MATERIALIZED_PERSONS,
                readLittleEndianInt64(FIXTURE_ROOT.resolve("vertex/person/vertex_count")));

        ArrivalClassifier classifier =
                ArrivalClassifier.of(
                        edgeInfo,
                        AdjListType.ordered_by_source,
                        AdjacencyFrontier.afterVertices(MATERIALIZED_PERSONS));
        ClassifiedArrivals classified = classifier.partition(SOURCES, TARGETS, SOURCES.length);

        assertEquals(2, classified.appendTail().count());
        assertArrayEquals(
                new long[] {MATERIALIZED_PERSONS, MATERIALIZED_PERSONS + 3},
                classified.appendTail().sources());
        assertArrayEquals(
                new long[] {0, MATERIALIZED_PERSONS + 4}, classified.appendTail().targets());
        assertEquals(2, classified.patches().count());
        assertArrayEquals(new long[] {297, 7}, classified.patches().sources());
        assertArrayEquals(
                new long[] {10, MATERIALIZED_PERSONS + 1}, classified.patches().targets());
        assertArrayEquals(new long[] {0, 2}, classified.patchedVertexChunks());
        assertFalse(classified.appendTail().isEmpty());

        assertEquals(
                new OrderedAdjacencyResolver(edgeInfo, AdjListType.ordered_by_source)
                        .locate(297)
                        .vertexChunkIndex(),
                classified.patchedVertexChunks()[1]);
        assertEquals(100, classifier.ordering().vertexChunkSize());
        assertEquals(1024, classifier.ordering().edgeChunkSize());
    }

    @Test
    public void movesTheSameBatchBetweenGroupsWhenTheOrderingChanges() throws Exception {
        EdgeInfo edgeInfo = loadPersonKnowsPerson();
        ClassifiedArrivals classified =
                ArrivalClassifier.of(
                                edgeInfo,
                                AdjListType.ordered_by_dest,
                                AdjacencyFrontier.afterVertices(MATERIALIZED_PERSONS))
                        .partition(SOURCES, TARGETS, SOURCES.length);

        assertEquals(2, classified.appendTail().count());
        assertArrayEquals(
                new long[] {7, MATERIALIZED_PERSONS + 3}, classified.appendTail().sources());
        assertArrayEquals(new long[] {297, MATERIALIZED_PERSONS}, classified.patches().sources());
        assertArrayEquals(new long[] {0}, classified.patchedVertexChunks());
        assertEquals(AdjListType.ordered_by_dest, classified.ordering().adjListType());
    }

    @Test
    public void readsTheFrontierAsAnExactOrderingKey() throws Exception {
        EdgeInfo edgeInfo = loadPersonKnowsPerson();

        ArrivalClassifier onEmpty =
                ArrivalClassifier.of(
                        edgeInfo, AdjListType.ordered_by_source, AdjacencyFrontier.empty());
        assertEquals(
                SOURCES.length,
                onEmpty.partition(SOURCES, TARGETS, SOURCES.length).appendTail().count());
        assertTrue(onEmpty.partition(SOURCES, TARGETS, SOURCES.length).patches().isEmpty());
        assertArrayEquals(
                new long[0],
                onEmpty.partition(SOURCES, TARGETS, SOURCES.length).patchedVertexChunks());

        ArrivalClassifier withinLastVertex =
                ArrivalClassifier.of(
                        edgeInfo,
                        AdjListType.ordered_by_source,
                        AdjacencyFrontier.at(MATERIALIZED_PERSONS - 1, 50));
        assertEquals(ArrivalClass.PATCH, withinLastVertex.classify(MATERIALIZED_PERSONS - 1, 49));
        assertEquals(
                ArrivalClass.APPEND_TAIL, withinLastVertex.classify(MATERIALIZED_PERSONS - 1, 50));
        assertEquals(ArrivalClass.APPEND_TAIL, withinLastVertex.classify(MATERIALIZED_PERSONS, 0));
        assertEquals(
                ArrivalClass.PATCH,
                withinLastVertex.classify(MATERIALIZED_PERSONS - 2, Long.MAX_VALUE));
    }

    @Test
    public void refusesInputThatIsNotAddressableInTheLayout() throws Exception {
        EdgeInfo edgeInfo = loadPersonKnowsPerson();
        ArrivalClassifier classifier =
                ArrivalClassifier.of(
                        edgeInfo,
                        AdjListType.ordered_by_source,
                        AdjacencyFrontier.afterVertices(MATERIALIZED_PERSONS));

        assertThrows(IllegalArgumentException.class, () -> classifier.classify(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> classifier.classify(0, -1));
        assertThrows(
                IllegalArgumentException.class,
                () -> classifier.partition(SOURCES, TARGETS, SOURCES.length + 1));
        assertThrows(
                IllegalArgumentException.class, () -> classifier.partition(SOURCES, TARGETS, -1));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        classifier
                                .partition(SOURCES, TARGETS, SOURCES.length)
                                .appendTail()
                                .source(2));
        assertThrows(
                IllegalArgumentException.class,
                () -> AdjacencyOrdering.of(edgeInfo, AdjListType.unordered_by_source));
        assertThrows(IllegalArgumentException.class, () -> AdjacencyFrontier.afterVertices(-1));
        assertThrows(IllegalArgumentException.class, () -> AdjacencyFrontier.at(0, -1));
        assertEquals(0, classifier.partition(SOURCES, TARGETS, 0).appendTail().count());
    }

    private static EdgeInfo loadPersonKnowsPerson() throws Exception {
        return new LocalFileSystemStringGraphInfoLoader()
                .loadEdgeInfo(FIXTURE_ROOT.resolve("person_knows_person.edge.yml").toUri());
    }

    private static long readLittleEndianInt64(Path path) throws IOException {
        return ByteBuffer.wrap(Files.readAllBytes(path)).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
}
