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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

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
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Row;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Growing a vertex type by writing the partitions the arrival lands in, not the whole group.
 *
 * <p>The claim under test is what makes append-first ingest affordable: publishing vertices that
 * sort past everything materialized touches the partition the group stopped inside and the
 * partitions after it, and leaves every earlier chunk byte-identical.
 */
public class GraphWriterVertexGrowthTest {
    private static final Schema SOURCE_SCHEMA =
            new Schema(List.of(new Field("id", ColumnType.of(ColumnType.Kind.INT64), false)));

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void growsAVertexTypeByRewritingOnlyThePartitionsTheArrivalLandsIn() throws Exception {
        Path rootPath = temporaryFolder.newFolder("growth").toPath();
        URI root = rootPath.toUri();
        Definition definition = definition(root);
        LocalStorage storage = new LocalStorage();
        GraphWriter writer =
                new GraphWriter(
                        definition.graphInfo,
                        root,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        WriteMode.OVERWRITE);

        assertEquals(
                7,
                writer.writeVertexPropertyGroup(
                        definition.vertexInfo, definition.vertexGroup, ids(0, 7)));
        assertEquals(7, littleEndianLong(rootPath.resolve("vertex/person/vertex_count")));
        byte[] untouched = Files.readAllBytes(chunk(rootPath, 0));

        assertEquals(
                2,
                writer.writeVertexPartition(
                        definition.vertexInfo, definition.vertexGroup, 3, ids(6, 8)));
        assertEquals(
                2,
                writer.writeVertexPartition(
                        definition.vertexInfo, definition.vertexGroup, 4, ids(8, 10)));
        writer.writeVertexCount(definition.vertexInfo, 10);

        assertArrayEquals(untouched, Files.readAllBytes(chunk(rootPath, 0)));
        assertEquals(10, littleEndianLong(rootPath.resolve("vertex/person/vertex_count")));
        assertEquals(List.of("0=0", "1=1"), chunkRows(storage, rootPath, 0));
        assertEquals(List.of("6=6", "7=7"), chunkRows(storage, rootPath, 3));
        assertEquals(List.of("8=8", "9=9"), chunkRows(storage, rootPath, 4));
    }

    @Test
    public void refusesAPartitionLargerThanTheChunkOrAWriterThatCannotOverwrite() throws Exception {
        Path rootPath = temporaryFolder.newFolder("refusal").toPath();
        URI root = rootPath.toUri();
        Definition definition = definition(root);
        LocalStorage storage = new LocalStorage();
        GraphWriter overwriting =
                new GraphWriter(
                        definition.graphInfo,
                        root,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        WriteMode.OVERWRITE);
        GraphWriter creating =
                new GraphWriter(
                        definition.graphInfo, root, storage, new ParquetPhysicalWriter(storage));

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        overwriting.writeVertexPartition(
                                definition.vertexInfo, definition.vertexGroup, 0, ids(0, 3)));
        assertThrows(
                IllegalStateException.class,
                () ->
                        creating.writeVertexPartition(
                                definition.vertexInfo, definition.vertexGroup, 0, ids(0, 2)));
    }

    private static Path chunk(Path rootPath, int chunk) {
        return rootPath.resolve("vertex/person/id/chunk" + chunk);
    }

    private static List<String> chunkRows(LocalStorage storage, Path rootPath, int chunk)
            throws IOException {
        List<String> values = new ArrayList<>();
        ParquetPhysicalReader reader = new ParquetPhysicalReader(storage);
        try (BatchCursor cursor =
                reader.read(ReadRequest.builder(chunk(rootPath, chunk).toUri()).build()).cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                for (int index = 0; index < batch.rowCount(); index++) {
                    Row row = batch.row(index);
                    values.add(row.value(0) + "=" + row.value(1));
                }
            }
        }
        return values;
    }

    private static long littleEndianLong(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        assertEquals(Long.BYTES, bytes.length);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static BatchCursor ids(long from, long toExclusive) {
        List<Row> rows = new ArrayList<>();
        for (long id = from; id < toExclusive; id++) {
            long value = id;
            rows.add(index -> value);
        }
        RecordBatch batch =
                new RecordBatch() {
                    @Override
                    public Schema schema() {
                        return SOURCE_SCHEMA;
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

    private static Definition definition(URI root) {
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
                                        AdjListType.ordered_by_source,
                                        FileType.PARQUET,
                                        "ordered_by_source/")),
                        List.of(
                                new PropertyGroup(
                                        List.of(
                                                new Property(
                                                        "weight", DataType.DOUBLE, false, false)),
                                        FileType.PARQUET,
                                        "weight/")));
        GraphInfo graph =
                new GraphInfo(
                        "tiny",
                        Map.of(URI.create("person.vertex.yml"), vertex),
                        Map.of(URI.create("person_knows_person.edge.yml"), edge),
                        root,
                        "gar/v1");
        return new Definition(graph, vertex, vertexGroup);
    }

    private static final class Definition {
        private final GraphInfo graphInfo;
        private final VertexInfo vertexInfo;
        private final PropertyGroup vertexGroup;

        private Definition(GraphInfo graphInfo, VertexInfo vertexInfo, PropertyGroup vertexGroup) {
            this.graphInfo = graphInfo;
            this.vertexInfo = vertexInfo;
            this.vertexGroup = vertexGroup;
        }
    }
}
