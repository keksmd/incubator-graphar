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

package org.apache.graphar.validator;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.ValueVector;
import org.apache.graphar.io.VectorRecordBatch;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.storage.local.LocalStorage;
import org.apache.graphar.writer.EdgeRecord;
import org.apache.graphar.writer.GraphWriter;

/**
 * Produces the generated all-layout dataset used by the Java TCK. Supply {@code
 * -Dgraphar.java.tck.outputDirectory=/absolute/path} to retain its output for C++ or Spark.
 */
final class TckDatasetExporter {
    static final String OUTPUT_DIRECTORY_PROPERTY = "graphar.java.tck.outputDirectory";

    private TckDatasetExporter() {}

    static GeneratedDataset write(Path root) throws IOException {
        if (Files.exists(root)) {
            try (Stream<Path> files = Files.list(root)) {
                if (files.findAny().isPresent()) {
                    throw new IllegalArgumentException(
                            "TCK output directory must be empty: " + root);
                }
            }
        }
        Files.createDirectories(root);
        URI rootUri = root.toUri();
        PropertyGroup vertexGroup =
                new PropertyGroup(
                        List.of(new Property("name", DataType.STRING, false, false)),
                        FileType.PARQUET,
                        "properties/");
        VertexInfo vertex =
                new VertexInfo(
                        "person", 3, List.of(vertexGroup), URI.create("vertex/person/"), "gar/v1");
        PropertyGroup weight =
                new PropertyGroup(
                        List.of(new Property("weight", DataType.DOUBLE, false, false)),
                        FileType.PARQUET,
                        "weight/");
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
                        3,
                        3,
                        true,
                        URI.create("edge/person_knows_person/"),
                        "gar/v1",
                        layouts,
                        List.of(weight));
        GraphInfo info =
                new GraphInfo(
                        "generated-validator",
                        Map.of(URI.create("person.vertex.yml"), vertex),
                        Map.of(URI.create("person_knows_person.edge.yml"), edge),
                        rootUri,
                        "gar/v1");
        LocalStorage storage = new LocalStorage();
        GraphWriter writer =
                new GraphWriter(
                        info,
                        rootUri,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        WriteMode.OVERWRITE);
        writer.writeVertexPropertyGroup(vertex, vertexGroup, rows(vertexSchema(), 6));
        List<EdgeRecord> edges = new ArrayList<>();
        for (int index = 0; index < 17; index++) {
            edges.add(
                    new EdgeRecord(
                            Math.floorMod(index * 5L, 6),
                            Math.floorMod(index * 3L + 1, 6),
                            Map.of("weight", index + 0.5D)));
        }
        for (AdjListType layout : AdjListType.values()) {
            writer.writeEdgeLayout(edge, layout, 6, edges);
        }
        Path graphYaml = root.resolve("generated.graph.yml");
        writer.writeMetadata(graphYaml.toUri());
        return new GeneratedDataset(root, rootUri, graphYaml, info, storage);
    }

    static Path retainedDirectory() {
        String value = System.getProperty(OUTPUT_DIRECTORY_PROPERTY);
        return value == null || value.isBlank() ? null : Path.of(value).toAbsolutePath();
    }

    private static Schema vertexSchema() {
        return new Schema(List.of(new Field("name", ColumnType.of(ColumnType.Kind.STRING), false)));
    }

    private static BatchCursor rows(Schema schema, int count) {
        List<Object> values = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            String value = "person-" + index;
            values.add(value);
        }
        Field field = schema.fields().get(0);
        ValueVector vector =
                new ValueVector() {
                    @Override
                    public Field field() {
                        return field;
                    }

                    @Override
                    public int valueCount() {
                        return values.size();
                    }

                    @Override
                    public boolean isNull(int index) {
                        return values.get(index) == null;
                    }

                    @Override
                    public Object getObject(int index) {
                        return values.get(index);
                    }
                };
        RecordBatch batch = new VectorRecordBatch(schema, List.of(vector), values.size());
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

    static final class GeneratedDataset {
        final Path path;
        final URI root;
        final Path graphYaml;
        final GraphInfo info;
        final LocalStorage storage;

        private GeneratedDataset(
                Path path, URI root, Path graphYaml, GraphInfo info, LocalStorage storage) {
            this.path = path;
            this.root = root;
            this.graphYaml = graphYaml;
            this.info = info;
            this.storage = storage;
        }
    }
}
