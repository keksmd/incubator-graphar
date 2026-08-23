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
import java.net.URI;
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
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.ValueVector;
import org.apache.graphar.io.VectorRecordBatch;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.reader.GraphReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.apache.graphar.writer.EdgeRecord;
import org.apache.graphar.writer.EdgeWriteOptions;
import org.apache.graphar.writer.EdgeWriteStats;
import org.apache.graphar.writer.GraphWriter;

/** One synthetic GraphAr dataset on the local file system, written by the pure-Java writer. */
final class CompactionDataset {
    static final String SRC_TYPE = "person";
    static final String EDGE_TYPE = "knows";
    static final String DST_TYPE = "person";
    static final String WEIGHT = "weight";
    static final AdjListType LAYOUT = AdjListType.ordered_by_source;

    private final Path rootPath;
    private final URI root;
    private final LocalStorage storage = new LocalStorage();
    private final GraphInfo graphInfo;
    private final VertexInfo vertexInfo;
    private final PropertyGroup vertexGroup;
    private final EdgeInfo edgeInfo;

    CompactionDataset(Path rootPath, long vertexChunkSize, long edgeChunkSize) {
        this.rootPath = rootPath;
        this.root = rootPath.toUri();
        this.vertexGroup =
                new PropertyGroup(
                        List.of(new Property("id", DataType.INT64, true, false)),
                        FileType.PARQUET,
                        "id/");
        this.vertexInfo =
                new VertexInfo(
                        SRC_TYPE,
                        vertexChunkSize,
                        List.of(vertexGroup),
                        URI.create("vertex/person/"),
                        "gar/v1");
        PropertyGroup edgeGroup =
                new PropertyGroup(
                        List.of(new Property(WEIGHT, DataType.DOUBLE, false, false)),
                        FileType.PARQUET,
                        "weight/");
        this.edgeInfo =
                new EdgeInfo(
                        SRC_TYPE,
                        EDGE_TYPE,
                        DST_TYPE,
                        edgeChunkSize,
                        vertexChunkSize,
                        vertexChunkSize,
                        true,
                        URI.create("edge/person_knows_person/"),
                        "gar/v1",
                        List.of(new AdjacentList(LAYOUT, FileType.PARQUET, "ordered_by_source/")),
                        List.of(edgeGroup));
        this.graphInfo =
                new GraphInfo(
                        "compaction",
                        Map.of(URI.create("person.vertex.yml"), vertexInfo),
                        Map.of(URI.create("person_knows_person.edge.yml"), edgeInfo),
                        root,
                        "gar/v1");
    }

    /** Returns one topology row with a property value derived from its endpoints. */
    static EdgeRecord edge(long source, long destination) {
        return new EdgeRecord(
                source, destination, Map.of(WEIGHT, (double) (source * 1000L + destination)));
    }

    Path rootPath() {
        return rootPath;
    }

    URI root() {
        return root;
    }

    LocalStorage storage() {
        return storage;
    }

    EdgeInfo edgeInfo() {
        return edgeInfo;
    }

    EdgeChunk chunk(long partition) {
        return new EdgeChunk(SRC_TYPE, EDGE_TYPE, DST_TYPE, LAYOUT, partition);
    }

    GraphWriter writer() {
        return writer(WriteMode.OVERWRITE);
    }

    GraphWriter writer(WriteMode mode) {
        return new GraphWriter(graphInfo, root, storage, new ParquetPhysicalWriter(storage), mode);
    }

    GraphReader reader() {
        return new GraphReader(graphInfo, root, storage, new ParquetPhysicalReader(storage));
    }

    /** Writes the vertex property group so a projection can be built over this dataset. */
    void writeVertices(long vertexCount) throws IOException {
        List<Object[]> values = new ArrayList<>();
        for (long vertex = 0; vertex < vertexCount; vertex++) {
            values.add(new Object[] {vertex});
        }
        writer().writeVertexPropertyGroup(
                        vertexInfo,
                        vertexGroup,
                        rows(
                                new Schema(
                                        List.of(
                                                new Field(
                                                        "id",
                                                        ColumnType.of(ColumnType.Kind.INT64),
                                                        false))),
                                values));
    }

    /** Writes the whole adjacency layout, which is what a compaction is measured against. */
    EdgeWriteStats writeLayout(long vertexCount, Iterable<EdgeRecord> records) throws IOException {
        return writer().writeEdgeLayout(
                        edgeInfo, LAYOUT, vertexCount, records, EdgeWriteOptions.defaults());
    }

    private static BatchCursor rows(Schema schema, List<Object[]> values) {
        List<ValueVector> columns = new ArrayList<>(schema.fields().size());
        for (int column = 0; column < schema.fields().size(); column++) {
            Object[] columnValues = new Object[values.size()];
            for (int row = 0; row < values.size(); row++) {
                Object[] rowValues = values.get(row);
                if (rowValues == null || rowValues.length != schema.fields().size()) {
                    throw new IllegalArgumentException("Every fixture row must match the schema.");
                }
                columnValues[row] = rowValues[column];
            }
            columns.add(new ObjectArrayVector(schema.fields().get(column), columnValues));
        }
        RecordBatch batch = new VectorRecordBatch(schema, columns, values.size());
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

    private static final class ObjectArrayVector implements ValueVector {
        private final Field field;
        private final Object[] values;

        private ObjectArrayVector(Field field, Object[] values) {
            this.field = field;
            this.values = values;
        }

        @Override
        public Field field() {
            return field;
        }

        @Override
        public int valueCount() {
            return values.length;
        }

        @Override
        public boolean isNull(int index) {
            return values[index] == null;
        }

        @Override
        public Object getObject(int index) {
            return values[index];
        }
    }
}
