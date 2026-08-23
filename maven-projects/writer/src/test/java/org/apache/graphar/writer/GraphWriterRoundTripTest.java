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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.graphar.info.AdjacentList;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.info.VertexInfo;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.reader.EdgeCursor;
import org.apache.graphar.reader.GraphReader;
import org.apache.graphar.reader.NeighborCursor;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Java-to-Java GraphAr Parquet writer and ordered-source reader compatibility gate. */
public class GraphWriterRoundTripTest {
    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void writesMetadataVertexChunksAndOrderedTopologyThatJavaReaderReopens()
            throws Exception {
        Path rootPath = temporaryFolder.newFolder("round-trip").toPath();
        URI root = rootPath.toUri();
        GraphDefinition definition = graphDefinition(root);
        LocalStorage storage = new LocalStorage();
        GraphWriter writer =
                new GraphWriter(
                        definition.graphInfo,
                        root,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        WriteMode.OVERWRITE);

        assertEquals(
                8,
                writer.writeVertexPropertyGroup(
                        definition.vertexInfo,
                        definition.vertexGroup,
                        rows(
                                new Schema(
                                        List.of(
                                                new Field(
                                                        "id",
                                                        ColumnType.of(ColumnType.Kind.INT64),
                                                        false))),
                                List.of(
                                        new Object[] {0L},
                                        new Object[] {1L},
                                        new Object[] {2L},
                                        new Object[] {3L},
                                        new Object[] {4L},
                                        new Object[] {5L},
                                        new Object[] {6L},
                                        new Object[] {7L}))));
        assertEquals(
                6,
                writer.writeOrderedSourceTopology(
                        definition.edgeInfo,
                        8,
                        List.of(
                                new TopologyEdge(0, 1),
                                new TopologyEdge(0, 4),
                                new TopologyEdge(2, 0),
                                new TopologyEdge(2, 1),
                                new TopologyEdge(2, 3),
                                new TopologyEdge(4, 2))));
        URI graphYaml = root.resolve("tiny.graph.yml");
        writer.writeMetadata(graphYaml);

        assertTrue(Files.isRegularFile(rootPath.resolve("tiny.graph.yml")));
        assertEquals(8, littleEndianLong(rootPath.resolve("vertex/person/vertex_count")));
        assertEquals(
                3,
                littleEndianLong(
                        rootPath.resolve(
                                "edge/person_knows_person/ordered_by_source/edge_count1")));
        assertEquals(
                0,
                littleEndianLong(
                        rootPath.resolve(
                                "edge/person_knows_person/ordered_by_source/edge_count3")));

        GraphReader reader =
                GraphReader.open(
                        graphYaml,
                        new LocalFileSystemStringGraphInfoLoader(),
                        storage,
                        new ParquetPhysicalReader(storage));
        assertEquals(2, reader.graphInfo().getVertexInfo("person").getChunkSize());
        assertEquals(8, reader.edge("person", "knows", "person").vertexCount());
        assertEquals(6, reader.edge("person", "knows", "person").edgeCount());
        assertEquals(List.of(0L, 1L, 3L), neighbors(reader, 2));
        assertEquals(List.of("0:1", "0:4", "2:0", "2:1", "2:3", "4:2"), scan(reader));
        assertEquals(
                List.of(6L, 7L),
                values(
                        new ParquetPhysicalReader(storage),
                        root.resolve(
                                definition.vertexInfo.getPropertyGroupChunkUri(
                                        definition.vertexGroup, 3))));
    }

    @Test
    public void rejectsUnsortedTopologyBeforeItCreatesControlFiles() throws Exception {
        Path rootPath = temporaryFolder.newFolder("invalid").toPath();
        URI root = rootPath.toUri();
        GraphDefinition definition = graphDefinition(root);
        LocalStorage storage = new LocalStorage();
        GraphWriter writer =
                new GraphWriter(
                        definition.graphInfo,
                        root,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        WriteMode.CREATE_NEW);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        writer.writeOrderedSourceTopology(
                                definition.edgeInfo,
                                4,
                                List.of(new TopologyEdge(2, 0), new TopologyEdge(1, 0))));
        assertFalse(
                Files.exists(
                        rootPath.resolve(
                                "edge/person_knows_person/ordered_by_source/vertex_count")));
    }

    private static GraphDefinition graphDefinition(URI root) {
        PropertyGroup vertexGroup =
                new PropertyGroup(
                        List.of(new Property("id", DataType.INT64, true, false)),
                        FileType.PARQUET,
                        "id/");
        VertexInfo vertex =
                new VertexInfo(
                        "person", 2, List.of(vertexGroup), URI.create("vertex/person/"), "gar/v1");
        PropertyGroup edgeProperties =
                new PropertyGroup(
                        List.of(new Property("weight", DataType.DOUBLE, false, false)),
                        FileType.PARQUET,
                        "weight/");
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
                                        AdjListType.ordered_by_source,
                                        FileType.PARQUET,
                                        "ordered_by_source/")),
                        List.of(edgeProperties));
        GraphInfo graph =
                new GraphInfo(
                        "tiny",
                        Map.of(URI.create("person.vertex.yml"), vertex),
                        Map.of(URI.create("person_knows_person.edge.yml"), edge),
                        root,
                        "gar/v1");
        return new GraphDefinition(graph, vertex, vertexGroup, edge);
    }

    private static long littleEndianLong(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        assertEquals(Long.BYTES, bytes.length);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static List<Long> neighbors(GraphReader reader, long source) throws IOException {
        List<Long> values = new ArrayList<>();
        try (NeighborCursor cursor = reader.edge("person", "knows", "person").neighbors(source)) {
            while (cursor.next()) values.add(cursor.destination());
        }
        return values;
    }

    private static List<String> scan(GraphReader reader) throws IOException {
        List<String> values = new ArrayList<>();
        try (EdgeCursor cursor = reader.edge("person", "knows", "person").scanEdges()) {
            while (cursor.next()) values.add(cursor.source() + ":" + cursor.destination());
        }
        return values;
    }

    private static List<Long> values(ParquetPhysicalReader reader, URI uri) throws IOException {
        List<Long> values = new ArrayList<>();
        try (BatchCursor cursor =
                reader.read(org.apache.graphar.io.ReadRequest.builder(uri).build()).cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                for (int index = 0; index < batch.rowCount(); index++)
                    values.add((Long) batch.column(0).getObject(index));
            }
        }
        return values;
    }

    private static BatchCursor rows(Schema schema, List<Object[]> values) {
        return WriterTestBatches.rows(schema, values);
    }

    private static final class GraphDefinition {
        private final GraphInfo graphInfo;
        private final VertexInfo vertexInfo;
        private final PropertyGroup vertexGroup;
        private final EdgeInfo edgeInfo;

        private GraphDefinition(
                GraphInfo graphInfo,
                VertexInfo vertexInfo,
                PropertyGroup vertexGroup,
                EdgeInfo edgeInfo) {
            this.graphInfo = graphInfo;
            this.vertexInfo = vertexInfo;
            this.vertexGroup = vertexGroup;
            this.edgeInfo = edgeInfo;
        }
    }
}
