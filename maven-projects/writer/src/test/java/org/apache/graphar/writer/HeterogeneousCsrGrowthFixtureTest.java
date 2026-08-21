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
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
import org.apache.graphar.io.Row;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.reader.CsrDirection;
import org.apache.graphar.reader.GraphReader;
import org.apache.graphar.reader.HeterogeneousCsr;
import org.apache.graphar.reader.VertexIdIndex;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Proves a projection extended by a batch that introduces vertices is the projection a full rebuild
 * over the grown dataset would produce.
 *
 * <p>An identity graph grows by vertices, not only by edges: a user, device, or address that was
 * never seen before is the mass case of an ingest. Growing the vertex space moves every type
 * declared after the one that grew, so the claim worth pinning is not that the new edges are there,
 * it is that the whole projection is indistinguishable from the rebuild, which is what these
 * compare against.
 */
public class HeterogeneousCsrGrowthFixtureTest {
    private static final List<String> TYPES = List.of("user", "device", "ip", "phone");
    private static final List<String> EDGE_NAMES =
            List.of("user_uses_device", "user_from_ip", "user_owns_phone");
    private static final Map<String, Integer> BASE_COUNTS =
            Map.of("user", 6, "device", 4, "ip", 3, "phone", 2);
    private static final Map<String, Integer> GROWN_COUNTS =
            Map.of("user", 8, "device", 5, "ip", 4, "phone", 3);

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private int datasetCounter;

    @Test
    public void aGrownProjectionEqualsARebuildOverTheGrownDataset() throws Exception {
        for (CsrDirection direction : CsrDirection.values()) {
            HeterogeneousCsr merged = build(BASE_COUNTS, baseEdges(), direction).merge(batch());
            HeterogeneousCsr rebuilt = build(GROWN_COUNTS, grownEdges(), direction);

            assertEquals(direction.name(), rebuilt.vertexTypes(), merged.vertexTypes());
            assertEquals(direction.name(), rebuilt.vertexCount(), merged.vertexCount());
            assertEquals(direction.name(), rebuilt.edgeCount(), merged.edgeCount());
            for (String type : TYPES) {
                assertEquals(direction.name() + ' ' + type, rebuilt.base(type), merged.base(type));
                for (int local = 0; local < GROWN_COUNTS.get(type); local++) {
                    String identifier = type + '-' + local;
                    assertEquals(
                            direction.name() + ' ' + identifier,
                            rebuilt.globalIndex(type, identifier),
                            merged.globalIndex(type, identifier));
                }
            }
            assertArrayEquals(direction.name(), rebuilt.csr().offsets(), merged.csr().offsets());
            assertArrayEquals(
                    direction.name(), rebuilt.csr().destinations(), merged.csr().destinations());
        }
    }

    @Test
    public void arrivingVerticesPushEveryLaterTypeAlong() throws Exception {
        HeterogeneousCsr base = build(BASE_COUNTS, baseEdges(), CsrDirection.UNDIRECTED);
        long deviceZero = base.globalIndex("device", "device-0");
        long[] deviceZeroNeighbors = base.neighbors(deviceZero);

        HeterogeneousCsr merged = base.merge(batch());

        assertEquals(0L, merged.base("user"));
        assertEquals(8L, merged.base("device"));
        assertEquals(13L, merged.base("ip"));
        assertEquals(17L, merged.base("phone"));
        assertEquals(20L, merged.vertexCount());
        assertEquals(deviceZero + 2L, merged.globalIndex("device", "device-0"));
        assertArrayEquals(
                "the projection being served must not change under a merge",
                deviceZeroNeighbors,
                base.neighbors(deviceZero));
        assertEquals(VertexIdIndex.ABSENT, base.globalIndex("user", "user-6"));
        assertTrue(merged.globalIndex("user", "user-6") >= 0);
        assertTrue(merged.globalIndex("phone", "phone-2") >= 0);
    }

    @Test
    public void aMergedAdjacencyStaysOrdered() throws Exception {
        HeterogeneousCsr merged =
                build(BASE_COUNTS, baseEdges(), CsrDirection.UNDIRECTED).merge(batch());

        for (long vertex = 0; vertex < merged.vertexCount(); vertex++) {
            long previous = -1L;
            for (long neighbor : merged.neighbors(vertex)) {
                assertTrue("adjacency of " + vertex + " must stay ordered", neighbor >= previous);
                previous = neighbor;
            }
        }
    }

    @Test
    public void aBatchNamingAnUndeclaredVertexTypeIsRefused() throws Exception {
        HeterogeneousCsr projection = build(BASE_COUNTS, baseEdges(), CsrDirection.UNDIRECTED);
        HeterogeneousCsr.MergeBatch batch =
                HeterogeneousCsr.batch().addEdge("user", "user-0", "browser", "browser-0");

        IllegalArgumentException refused =
                assertThrows(IllegalArgumentException.class, () -> projection.merge(batch));

        assertTrue(refused.getMessage(), refused.getMessage().contains("browser"));
    }

    /**
     * Returns the batch under test. Its arriving identifiers are first mentioned in the order the
     * grown dataset stores them, which is what an append-only writer produces.
     */
    private static HeterogeneousCsr.MergeBatch batch() {
        return HeterogeneousCsr.batch()
                .addEdge("user", "user-6", "device", "device-0")
                .addEdge("user", "user-0", "device", "device-4")
                .addEdge("user", "user-7", "ip", "ip-3")
                .addEdge("user", "user-2", "device", "device-4")
                .addEdge("user", "user-6", "phone", "phone-1")
                .addEdge("user", "user-1", "ip", "ip-0")
                .addVertex("phone", "phone-2");
    }

    private static List<long[]> baseEdges() {
        return List.of(
                new long[] {0, 0, 0},
                new long[] {0, 1, 0},
                new long[] {1, 0, 0},
                new long[] {2, 2, 0},
                new long[] {3, 3, 0},
                new long[] {0, 0, 1},
                new long[] {1, 0, 1},
                new long[] {4, 1, 1},
                new long[] {5, 2, 1},
                new long[] {0, 0, 2},
                new long[] {3, 1, 2});
    }

    /** Returns the base topology and the edges of {@link #batch}, in their grown local indexes. */
    private static List<long[]> grownEdges() {
        List<long[]> edges = new ArrayList<>(baseEdges());
        edges.add(new long[] {6, 0, 0});
        edges.add(new long[] {0, 4, 0});
        edges.add(new long[] {7, 3, 1});
        edges.add(new long[] {2, 4, 0});
        edges.add(new long[] {6, 1, 2});
        edges.add(new long[] {1, 0, 1});
        return edges;
    }

    private HeterogeneousCsr build(
            Map<String, Integer> counts, List<long[]> edges, CsrDirection direction)
            throws Exception {
        return HeterogeneousCsr.builder(write(counts, edges))
                .addVertexType("user")
                .addVertexType("device")
                .addVertexType("ip")
                .addVertexType("phone")
                .addEdgeType("user", "uses", "device")
                .addEdgeType("user", "from", "ip")
                .addEdgeType("user", "owns", "phone")
                .direction(direction)
                .build();
    }

    private GraphReader write(Map<String, Integer> counts, List<long[]> edges) throws Exception {
        Path rootPath = temporaryFolder.newFolder("identity-graph-" + datasetCounter++).toPath();
        URI root = rootPath.toUri();
        Map<String, VertexInfo> vertexInfos = new LinkedHashMap<>();
        Map<String, PropertyGroup> vertexGroups = new LinkedHashMap<>();
        for (String type : TYPES) {
            PropertyGroup group =
                    new PropertyGroup(
                            List.of(new Property("id", DataType.STRING, true, false)),
                            FileType.PARQUET,
                            "id/");
            vertexGroups.put(type, group);
            vertexInfos.put(
                    type,
                    new VertexInfo(
                            type, 2, List.of(group), URI.create("vertex/" + type + '/'), "gar/v1"));
        }

        Map<String, EdgeInfo> edgeInfos = new LinkedHashMap<>();
        edgeInfos.put("user_uses_device", edgeInfo("user", "uses", "device"));
        edgeInfos.put("user_from_ip", edgeInfo("user", "from", "ip"));
        edgeInfos.put("user_owns_phone", edgeInfo("user", "owns", "phone"));

        Map<URI, VertexInfo> vertexYaml = new LinkedHashMap<>();
        vertexInfos.forEach((type, info) -> vertexYaml.put(URI.create(type + ".vertex.yml"), info));
        Map<URI, EdgeInfo> edgeYaml = new LinkedHashMap<>();
        edgeInfos.forEach((name, info) -> edgeYaml.put(URI.create(name + ".edge.yml"), info));
        GraphInfo graphInfo = new GraphInfo("identity", vertexYaml, edgeYaml, root, "gar/v1");

        LocalStorage storage = new LocalStorage();
        GraphWriter writer =
                new GraphWriter(
                        graphInfo,
                        root,
                        storage,
                        new ParquetPhysicalWriter(storage),
                        WriteMode.OVERWRITE);
        for (Map.Entry<String, VertexInfo> entry : vertexInfos.entrySet()) {
            String type = entry.getKey();
            List<Object[]> values = new ArrayList<>();
            for (int index = 0; index < counts.get(type); index++) {
                values.add(new Object[] {type + '-' + index});
            }
            assertEquals(
                    counts.get(type).intValue(),
                    writer.writeVertexPropertyGroup(
                            entry.getValue(), vertexGroups.get(type), rows(idSchema(), values)));
        }

        Map<String, List<EdgeRecord>> recordsByEdge = new LinkedHashMap<>();
        for (String name : EDGE_NAMES) {
            recordsByEdge.put(name, new ArrayList<>());
        }
        for (long[] specification : edges) {
            recordsByEdge
                    .get(EDGE_NAMES.get(Math.toIntExact(specification[2])))
                    .add(
                            new EdgeRecord(
                                    specification[0], specification[1], Map.of("weight", 1.0D)));
        }
        for (Map.Entry<String, List<EdgeRecord>> entry : recordsByEdge.entrySet()) {
            writer.writeEdgeLayout(
                    edgeInfos.get(entry.getKey()),
                    AdjListType.ordered_by_source,
                    counts.get("user"),
                    entry.getValue());
        }
        return new GraphReader(graphInfo, root, storage, new ParquetPhysicalReader(storage));
    }

    private static EdgeInfo edgeInfo(String srcType, String edgeType, String dstType) {
        return new EdgeInfo(
                srcType,
                edgeType,
                dstType,
                2,
                2,
                2,
                true,
                URI.create("edge/" + srcType + '_' + edgeType + '_' + dstType + '/'),
                "gar/v1",
                List.of(
                        new AdjacentList(
                                AdjListType.ordered_by_source,
                                FileType.PARQUET,
                                "ordered_by_source/")),
                List.of(
                        new PropertyGroup(
                                List.of(new Property("weight", DataType.DOUBLE, false, true)),
                                FileType.PARQUET,
                                "weight/")));
    }

    private static Schema idSchema() {
        return new Schema(List.of(new Field("id", ColumnType.of(ColumnType.Kind.STRING), false)));
    }

    private static BatchCursor rows(Schema schema, List<Object[]> values) {
        List<Row> rows = new ArrayList<>();
        for (Object[] value : values) {
            rows.add(index -> value[index]);
        }
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
}
