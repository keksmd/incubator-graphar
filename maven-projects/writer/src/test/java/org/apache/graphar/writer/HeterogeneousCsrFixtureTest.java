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

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
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
 * Proves a heterogeneous traversal over the ADR-0010 node types on a programmatically written
 * GraphAr dataset.
 */
public class HeterogeneousCsrFixtureTest {
    private static final int USER_COUNT = 6;
    private static final int DEVICE_COUNT = 4;
    private static final int IP_COUNT = 3;
    private static final int PHONE_COUNT = 2;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private int datasetCounter;

    @Test
    public void assignsOneGlobalIdentifierSpaceOverEveryDeclaredVertexType() throws Exception {
        HeterogeneousCsr projection = build(CsrDirection.UNDIRECTED);

        assertEquals(List.of("user", "device", "ip", "phone"), projection.vertexTypes());
        assertEquals(USER_COUNT + DEVICE_COUNT + IP_COUNT + PHONE_COUNT, projection.vertexCount());
        assertEquals(0L, projection.base("user"));
        assertEquals(6L, projection.base("device"));
        assertEquals(10L, projection.base("ip"));
        assertEquals(13L, projection.base("phone"));

        assertEquals(0L, projection.globalIndex("user", "user-0"));
        assertEquals(6L, projection.globalIndex("device", "device-0"));
        assertEquals(12L, projection.globalIndex("ip", "ip-2"));
        assertEquals(14L, projection.globalIndex("phone", "phone-1"));
        assertEquals(VertexIdIndex.ABSENT, projection.globalIndex("phone", "device-0"));
    }

    @Test
    public void roundTripsEveryGlobalIdentifierBackToItsTypeAndDenseIndex() throws Exception {
        HeterogeneousCsr projection = build(CsrDirection.UNDIRECTED);

        Map<String, Integer> countByType =
                Map.of(
                        "user", USER_COUNT,
                        "device", DEVICE_COUNT,
                        "ip", IP_COUNT,
                        "phone", PHONE_COUNT);
        for (String type : projection.vertexTypes()) {
            for (long local = 0; local < countByType.get(type); local++) {
                long global = projection.globalIndex(type, type + '-' + local);
                assertEquals(type + '-' + local, type, projection.typeOf(global));
                assertEquals(type + '-' + local, local, projection.localIndex(global));
            }
        }
        assertThrows(IllegalArgumentException.class, () -> projection.typeOf(15L));
    }

    @Test
    public void mergesEveryEdgeTypeIntoOneUndirectedAdjacency() throws Exception {
        HeterogeneousCsr projection = build(CsrDirection.UNDIRECTED);
        Map<Long, List<Long>> oracle = expectedUndirectedAdjacency();

        assertEquals(2L * edgeSpecifications().size(), projection.edgeCount());
        for (long global = 0; global < projection.vertexCount(); global++) {
            assertEquals(
                    "adjacency of " + global,
                    oracle.getOrDefault(global, List.of()),
                    boxed(projection.neighbors(global)));
        }
    }

    @Test
    public void reachesEveryNodeTypeWithinTwoHopsOfOneUserIdentifier() throws Exception {
        HeterogeneousCsr projection = build(CsrDirection.UNDIRECTED);

        long start = projection.globalIndex("user", "user-0");
        TreeSet<Long> visited = new TreeSet<>();
        visited.add(start);
        List<Long> frontier = List.of(start);
        List<String> typesByHop = new ArrayList<>();
        for (int hop = 0; hop < 2; hop++) {
            List<Long> next = new ArrayList<>();
            for (long vertex : frontier) {
                for (long neighbor : projection.neighbors(vertex)) {
                    if (visited.add(neighbor)) {
                        next.add(neighbor);
                        typesByHop.add(projection.typeOf(neighbor));
                    }
                }
            }
            frontier = next;
        }

        assertEquals(
                List.of("device", "ip", "phone", "user"),
                new TreeSet<>(typesByHop).stream().toList());
        assertEquals(List.of(0L, 1L, 6L, 7L, 10L, 13L), new ArrayList<>(visited));
    }

    @Test
    public void directedProjectionKeepsOnlyTheStoredOrientation() throws Exception {
        HeterogeneousCsr outgoing = build(CsrDirection.OUTGOING);

        assertEquals(edgeSpecifications().size(), outgoing.edgeCount());
        assertArrayEquals(
                new long[] {6L, 7L, 10L, 13L},
                outgoing.neighbors(outgoing.globalIndex("user", "user-0")));
        assertArrayEquals(
                new long[0], outgoing.neighbors(outgoing.globalIndex("device", "device-0")));

        HeterogeneousCsr incoming = build(CsrDirection.INCOMING);
        assertArrayEquals(
                new long[] {0L, 1L},
                incoming.neighbors(incoming.globalIndex("device", "device-0")));
    }

    @Test
    public void rejectsAnEdgeTripletWhoseVertexTypeWasNotDeclared() throws Exception {
        GraphReader graph = write();
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        HeterogeneousCsr.builder(graph)
                                .addVertexType("user")
                                .addEdgeType("user", "uses", "device")
                                .build());
    }

    private HeterogeneousCsr build(CsrDirection direction) throws Exception {
        return HeterogeneousCsr.builder(write())
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

    private static List<long[]> edgeSpecifications() {
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

    private Map<Long, List<Long>> expectedUndirectedAdjacency() {
        Map<Long, List<Long>> adjacency = new HashMap<>();
        long[] bases = {0L, 6L, 10L, 13L};
        for (long[] specification : edgeSpecifications()) {
            long source = specification[0];
            long target = bases[Math.toIntExact(specification[2]) + 1] + specification[1];
            adjacency.computeIfAbsent(source, key -> new ArrayList<>()).add(target);
            adjacency.computeIfAbsent(target, key -> new ArrayList<>()).add(source);
        }
        for (List<Long> neighbors : adjacency.values()) {
            neighbors.sort(Long::compare);
        }
        return adjacency;
    }

    private static List<Long> boxed(long[] values) {
        List<Long> boxed = new ArrayList<>(values.length);
        for (long value : values) {
            boxed.add(value);
        }
        return boxed;
    }

    private GraphReader write() throws Exception {
        Path rootPath = temporaryFolder.newFolder("identity-graph-" + datasetCounter++).toPath();
        URI root = rootPath.toUri();
        Map<String, VertexInfo> vertexInfos = new LinkedHashMap<>();
        Map<String, PropertyGroup> vertexGroups = new LinkedHashMap<>();
        Map<String, Integer> counts =
                new LinkedHashMap<>(
                        Map.of(
                                "user", USER_COUNT,
                                "device", DEVICE_COUNT,
                                "ip", IP_COUNT,
                                "phone", PHONE_COUNT));
        for (String type : List.of("user", "device", "ip", "phone")) {
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
        recordsByEdge.put("user_uses_device", new ArrayList<>());
        recordsByEdge.put("user_from_ip", new ArrayList<>());
        recordsByEdge.put("user_owns_phone", new ArrayList<>());
        List<String> edgeNames = List.of("user_uses_device", "user_from_ip", "user_owns_phone");
        for (long[] specification : edgeSpecifications()) {
            recordsByEdge
                    .get(edgeNames.get(Math.toIntExact(specification[2])))
                    .add(
                            new EdgeRecord(
                                    specification[0], specification[1], Map.of("weight", 1.0D)));
        }
        for (Map.Entry<String, List<EdgeRecord>> entry : recordsByEdge.entrySet()) {
            writer.writeEdgeLayout(
                    edgeInfos.get(entry.getKey()),
                    AdjListType.ordered_by_source,
                    USER_COUNT,
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
