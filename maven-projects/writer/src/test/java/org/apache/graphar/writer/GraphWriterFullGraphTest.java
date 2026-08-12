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

package org.apache.graphar.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.graphar.info.AdjacentList;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.Cardinality;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Row;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.reader.EdgePropertyCursor;
import org.apache.graphar.reader.GraphEdge;
import org.apache.graphar.reader.GraphReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Programmatically generated four-layout GraphAr compatibility test. */
public class GraphWriterFullGraphTest {
    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void writesAllLayoutsListsAndPositionAlignedProperties() throws Exception {
        Path rootPath = temporaryFolder.newFolder("full-graph").toPath();
        URI root = rootPath.toUri();
        Definition definition = definition(root);
        LocalStorage storage = new LocalStorage();
        GraphWriter writer =
                new GraphWriter(
                        definition.graph,
                        root,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        WriteMode.OVERWRITE);

        assertEquals(
                7,
                writer.writeVertexPropertyGroup(
                        definition.vertex,
                        definition.vertexProperties,
                        rows(
                                vertexSchema(),
                                List.of(
                                        new Object[] {List.of("a", "b"), List.of("x", "y")},
                                        new Object[] {List.of(), List.of("y")},
                                        new Object[] {null, null},
                                        new Object[] {List.of("c"), List.of("z")},
                                        new Object[] {List.of("d", "e"), List.of("p", "q")},
                                        new Object[] {List.of(), List.of()},
                                        new Object[] {List.of("f"), List.of("final")}))));

        List<EdgeRecord> records =
                List.of(
                        new EdgeRecord(0, 1, Map.of("weight", 1.0D, "kind", "first")),
                        new EdgeRecord(0, 1, Map.of("weight", 2.0D, "kind", "parallel")),
                        new EdgeRecord(2, 0, Map.of("weight", 3.0D, "kind", "reverse")),
                        new EdgeRecord(3, 4, Map.of("weight", 4.0D, "kind", "tail")),
                        new EdgeRecord(4, 3, Map.of("weight", 5.0D, "kind", "last")));
        for (AdjListType layout : AdjListType.values()) {
            assertEquals(5, writer.writeEdgeLayout(definition.edge, layout, 7, records));
        }

        GraphReader reader =
                new GraphReader(
                        definition.graph, root, storage, new ParquetPhysicalReader(storage));
        for (AdjListType layout : AdjListType.values()) {
            assertEquals(7, reader.edge("person", "knows", "person", layout).vertexCount());
            assertEquals(5, reader.edge("person", "knows", "person", layout).edgeCount());
            assertEquals(5, scan(reader, layout).size());
        }
        assertEquals(
                List.of("0:1:first", "0:1:parallel"),
                selected(reader, AdjListType.ordered_by_source, 0));
        assertEquals(
                List.of("0:1:first", "0:1:parallel"),
                selected(reader, AdjListType.unordered_by_dest, 1));
        assertFalse(
                Files.exists(
                        rootPath.resolve(
                                "edge/person_knows_person/unordered_by_source/offset/chunk0")));
        assertTrue(
                Files.exists(
                        rootPath.resolve(
                                "edge/person_knows_person/ordered_by_dest/offset/chunk0")));
        assertEquals(
                List.of("a", "b"),
                firstValue(
                        new ParquetPhysicalReader(storage),
                        root.resolve(
                                definition.vertex.getPropertyGroupChunkUri(
                                        definition.vertexProperties, 0)),
                        1));
        assertEquals(
                0L,
                firstValue(
                        new ParquetPhysicalReader(storage),
                        root.resolve(
                                definition.vertex.getPropertyGroupChunkUri(
                                        definition.vertexProperties, 0)),
                        0));
    }

    @Test
    public void rejectsDuplicateSetValuesBeforeWritingVertexChunks() throws Exception {
        Path rootPath = temporaryFolder.newFolder("duplicate-set").toPath();
        URI root = rootPath.toUri();
        Definition definition = definition(root);
        LocalStorage storage = new LocalStorage();
        GraphWriter writer =
                new GraphWriter(
                        definition.graph,
                        root,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        WriteMode.OVERWRITE);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        writer.writeVertexPropertyGroup(
                                definition.vertex,
                                definition.vertexProperties,
                                rows(
                                        vertexSchema(),
                                        Collections.singletonList(
                                                new Object[] {
                                                    List.of("valid"),
                                                    List.of("duplicate", "duplicate")
                                                }))));
        assertFalse(Files.exists(rootPath.resolve("vertex/person/attributes/chunk0")));
    }

    @Test
    public void streamsExternalSortRunsWithoutRetainingTheWholeEdgeSource() throws Exception {
        Path rootPath = temporaryFolder.newFolder("streaming-graph").toPath();
        URI root = rootPath.toUri();
        Definition definition = definition(root);
        LocalStorage storage = new LocalStorage();
        GraphWriter writer =
                new GraphWriter(
                        definition.graph,
                        root,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        WriteMode.OVERWRITE);

        GraphReader reader =
                new GraphReader(
                        definition.graph, root, storage, new ParquetPhysicalReader(storage));
        for (AdjListType layout : AdjListType.values()) {
            EdgeWriteStats stats =
                    writer.writeEdgeLayout(
                            definition.edge,
                            layout,
                            13,
                            generatedEdges(97),
                            new EdgeWriteOptions(3));
            assertEquals(97, stats.edgeCount());
            assertEquals(7, stats.partitionCount());
            assertTrue(stats.peakRecordsBuffered() <= 3);
            if (layout.isOrdered()) {
                assertTrue(stats.spillRunCount() > 7);
            }
            assertEquals(97, scan(reader, layout).size());
        }
        assertEquals(8, selected(reader, AdjListType.ordered_by_source, 0).size());
    }

    @Test
    public void compactsMoreThanOneMergeFanInOfSortedRuns() throws Exception {
        Path rootPath = temporaryFolder.newFolder("multi-pass-merge").toPath();
        URI root = rootPath.toUri();
        Definition definition = definition(root);
        LocalStorage storage = new LocalStorage();
        GraphWriter writer =
                new GraphWriter(
                        definition.graph,
                        root,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        WriteMode.OVERWRITE);

        EdgeWriteStats stats =
                writer.writeEdgeLayout(
                        definition.edge,
                        AdjListType.ordered_by_source,
                        1,
                        repeatedSourceEdges(100),
                        new EdgeWriteOptions(3));

        assertEquals(100, stats.edgeCount());
        assertTrue(stats.spillRunCount() >= 36);
        assertTrue(stats.peakRecordsBuffered() <= 3);
        GraphReader reader =
                new GraphReader(
                        definition.graph, root, storage, new ParquetPhysicalReader(storage));
        assertEquals(100, scan(reader, AdjListType.ordered_by_source).size());
    }

    private static Iterable<EdgeRecord> generatedEdges(int count) {
        return () ->
                new Iterator<EdgeRecord>() {
                    private int index;

                    @Override
                    public boolean hasNext() {
                        return index < count;
                    }

                    @Override
                    public EdgeRecord next() {
                        int current = index++;
                        long source = Math.floorMod(current * 7L, 13);
                        return new EdgeRecord(
                                source,
                                Math.floorMod(current * 11L + 1, 13),
                                Map.of("weight", (double) current, "kind", "edge-" + current));
                    }
                };
    }

    private static Iterable<EdgeRecord> repeatedSourceEdges(int count) {
        return () ->
                new Iterator<EdgeRecord>() {
                    private int index;

                    @Override
                    public boolean hasNext() {
                        return index < count;
                    }

                    @Override
                    public EdgeRecord next() {
                        int current = index++;
                        return new EdgeRecord(
                                0,
                                0,
                                Map.of("weight", (double) current, "kind", "edge-" + current));
                    }
                };
    }

    private static List<String> scan(GraphReader reader, AdjListType layout) throws Exception {
        List<String> values = new ArrayList<>();
        try (EdgePropertyCursor cursor =
                reader.edge("person", "knows", "person", layout).scanEdges()) {
            while (cursor.next()) {
                GraphEdge edge = cursor.edge();
                values.add(
                        edge.source()
                                + ":"
                                + edge.destination()
                                + ":"
                                + edge.properties().get("kind"));
            }
        }
        return values;
    }

    private static List<String> selected(GraphReader reader, AdjListType layout, long vertex)
            throws Exception {
        List<String> values = new ArrayList<>();
        try (EdgePropertyCursor cursor =
                reader.edge("person", "knows", "person", layout).edges(vertex)) {
            while (cursor.next()) {
                GraphEdge edge = cursor.edge();
                values.add(
                        edge.source()
                                + ":"
                                + edge.destination()
                                + ":"
                                + edge.properties().get("kind"));
            }
        }
        return values;
    }

    private static Object firstValue(ParquetPhysicalReader reader, URI uri, int column)
            throws Exception {
        try (BatchCursor cursor =
                reader.read(org.apache.graphar.io.ReadRequest.builder(uri).build()).cursor()) {
            assertTrue(cursor.next());
            return cursor.batch().row(0).value(column);
        }
    }

    private static Schema vertexSchema() {
        return new Schema(
                List.of(
                        new Field(
                                "tags",
                                ColumnType.listOf(ColumnType.of(ColumnType.Kind.STRING)),
                                true),
                        new Field(
                                "labels",
                                ColumnType.listOf(ColumnType.of(ColumnType.Kind.STRING)),
                                true)));
    }

    private static Definition definition(URI root) {
        PropertyGroup vertexProperties =
                new PropertyGroup(
                        List.of(
                                new Property(
                                        "tags",
                                        DataType.listOf(DataType.STRING),
                                        Cardinality.SINGLE,
                                        false,
                                        true),
                                new Property(
                                        "labels", DataType.STRING, Cardinality.SET, false, true)),
                        FileType.PARQUET,
                        "attributes/");
        VertexInfo vertex =
                new VertexInfo(
                        "person",
                        2,
                        List.of(vertexProperties),
                        URI.create("vertex/person/"),
                        "gar/v1");
        PropertyGroup weights =
                new PropertyGroup(
                        List.of(new Property("weight", DataType.DOUBLE, false, false)),
                        FileType.PARQUET,
                        "weight/");
        PropertyGroup kinds =
                new PropertyGroup(
                        List.of(new Property("kind", DataType.STRING, false, false)),
                        FileType.PARQUET,
                        "kind/");
        List<AdjacentList> layouts = new ArrayList<>();
        for (AdjListType layout : AdjListType.values()) {
            layouts.add(new AdjacentList(layout, FileType.PARQUET, layout + "/"));
        }
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
                        layouts,
                        List.of(weights, kinds));
        return new Definition(
                new GraphInfo(
                        "generated",
                        Map.of(URI.create("person.vertex.yml"), vertex),
                        Map.of(URI.create("person_knows_person.edge.yml"), edge),
                        root,
                        "gar/v1"),
                vertex,
                vertexProperties,
                edge);
    }

    private static BatchCursor rows(Schema schema, List<Object[]> values) {
        List<Row> rows = new ArrayList<>();
        for (Object[] value : values) rows.add(index -> value[index]);
        RecordBatch batch =
                new RecordBatch() {
                    @Override
                    public Schema schema() {
                        return schema;
                    }

                    @Override
                    public int rowCount() {
                        return rows.size();
                    }

                    @Override
                    public Row row(int index) {
                        return rows.get(index);
                    }
                };
        return new BatchCursor() {
            private boolean available = true;

            @Override
            public boolean next() {
                boolean result = available;
                available = false;
                return result;
            }

            @Override
            public RecordBatch batch() {
                return batch;
            }

            @Override
            public void close() {}
        };
    }

    private static final class Definition {
        private final GraphInfo graph;
        private final VertexInfo vertex;
        private final PropertyGroup vertexProperties;
        private final EdgeInfo edge;

        private Definition(
                GraphInfo graph, VertexInfo vertex, PropertyGroup vertexProperties, EdgeInfo edge) {
            this.graph = graph;
            this.vertex = vertex;
            this.vertexProperties = vertexProperties;
            this.edge = edge;
        }
    }
}
