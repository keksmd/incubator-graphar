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
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.apache.graphar.reader.BoundedTraversal;
import org.apache.graphar.reader.CsrDirection;
import org.apache.graphar.reader.GraphReader;
import org.apache.graphar.reader.HeterogeneousCsr;
import org.apache.graphar.reader.TraversalResult;
import org.apache.graphar.storage.Storage;
import org.apache.graphar.storage.local.LocalStorage;
import org.apache.graphar.storage.s3.S3Storage;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * Opt-in end-to-end test of an object-store dataset. Run with {@code
 * GRAPHAR_MINIO_ENDPOINT=http://localhost:19000}.
 *
 * <p>The storage module already proves its own transport. What this proves is the whole serving
 * path over that transport: an identity graph is written to the object store, reopened from it, and
 * projected into a heterogeneous adjacency, and the result is compared against the same dataset
 * written to a local directory. The two projections must be identical, so the object store is shown
 * to be equivalent rather than merely functional.
 */
public class S3IdentityGraphIntegrationIT {
    private static final String ENDPOINT = "GRAPHAR_MINIO_ENDPOINT";
    private static final String ACCESS_KEY = "GRAPHAR_MINIO_ACCESS_KEY";
    private static final String SECRET_KEY = "GRAPHAR_MINIO_SECRET_KEY";
    private static final List<String> TYPES = List.of("user", "device", "ip", "phone");
    private static final Map<String, Integer> COUNTS =
            Map.of("user", 6, "device", 4, "ip", 3, "phone", 2);

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void projectsTheSameIdentityGraphFromAnObjectStoreAsFromALocalDirectory()
            throws Exception {
        String endpoint = System.getenv(ENDPOINT);
        Assume.assumeTrue("Set " + ENDPOINT + " to run the object-store test.", endpoint != null);

        Path localRoot = temporaryFolder.newFolder("identity-local").toPath();
        HeterogeneousCsr local =
                project(write(new LocalStorage(), localRoot.toUri(), localRoot.toUri()));

        String bucket = "graphar-identity-" + UUID.randomUUID().toString().replace("-", "");
        Path staging = Files.createTempDirectory("graphar-identity-stage-");
        try (S3Client client = client(endpoint)) {
            client.createBucket(request -> request.bucket(bucket));
            S3Storage storage = new S3Storage(client, staging);
            URI root = URI.create("s3://" + bucket + "/identity/");

            HeterogeneousCsr remote = project(write(storage, root, root));

            assertEquals(local.vertexTypes(), remote.vertexTypes());
            assertEquals(local.vertexCount(), remote.vertexCount());
            assertEquals(local.edgeCount(), remote.edgeCount());
            for (String type : TYPES) {
                assertEquals(type, local.base(type), remote.base(type));
                for (int index = 0; index < COUNTS.get(type); index++) {
                    String identifier = type + '-' + index;
                    assertEquals(
                            identifier,
                            local.globalIndex(type, identifier),
                            remote.globalIndex(type, identifier));
                }
            }
            for (long vertex = 0; vertex < local.vertexCount(); vertex++) {
                assertArrayEquals(
                        "adjacency of " + vertex,
                        local.neighbors(vertex),
                        remote.neighbors(vertex));
            }

            TraversalResult localHops =
                    BoundedTraversal.neighborhood(
                            local.csr(), local.globalIndex("user", "user-0"), 2, 50);
            TraversalResult remoteHops =
                    BoundedTraversal.neighborhood(
                            remote.csr(), remote.globalIndex("user", "user-0"), 2, 50);
            assertArrayEquals(localHops.vertices(), remoteHops.vertices());
            assertArrayEquals(localHops.depths(), remoteHops.depths());
            assertTrue(remoteHops.size() > 1);
        } finally {
            deleteTree(staging);
        }
    }

    private static HeterogeneousCsr project(GraphReader graph) throws Exception {
        return HeterogeneousCsr.builder(graph)
                .addVertexType("user")
                .addVertexType("device")
                .addVertexType("ip")
                .addVertexType("phone")
                .addEdgeType("user", "uses", "device")
                .addEdgeType("user", "from", "ip")
                .addEdgeType("user", "owns", "phone")
                .direction(CsrDirection.UNDIRECTED)
                .build();
    }

    private static GraphReader write(Storage storage, URI root, URI infoRoot) throws Exception {
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
        GraphInfo graphInfo = new GraphInfo("identity", vertexYaml, edgeYaml, infoRoot, "gar/v1");

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
            for (int index = 0; index < COUNTS.get(type); index++) {
                values.add(new Object[] {type + '-' + index});
            }
            assertEquals(
                    COUNTS.get(type).intValue(),
                    writer.writeVertexPropertyGroup(
                            entry.getValue(), vertexGroups.get(type), rows(idSchema(), values)));
        }

        Map<String, List<EdgeRecord>> recordsByEdge = new LinkedHashMap<>();
        List<String> edgeNames = List.of("user_uses_device", "user_from_ip", "user_owns_phone");
        for (String name : edgeNames) {
            recordsByEdge.put(name, new ArrayList<>());
        }
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
                    COUNTS.get("user"),
                    entry.getValue());
        }
        return new GraphReader(graphInfo, root, storage, new ParquetPhysicalReader(storage));
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

    private static S3Client client(String endpoint) {
        String accessKey = System.getenv().getOrDefault(ACCESS_KEY, "minioadmin");
        String secretKey = System.getenv().getOrDefault(SECRET_KEY, "minioadmin");
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.US_EAST_1)
                .serviceConfiguration(
                        S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
    }

    private static void deleteTree(Path root) throws java.io.IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            for (Path path :
                    paths.sorted(java.util.Comparator.reverseOrder())
                            .collect(java.util.stream.Collectors.toList())) {
                Files.delete(path);
            }
        }
    }
}
