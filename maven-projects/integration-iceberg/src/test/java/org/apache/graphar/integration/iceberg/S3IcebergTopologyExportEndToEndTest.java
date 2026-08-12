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

package org.apache.graphar.integration.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.ReadCapability;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.reader.NeighborCursor;
import org.apache.graphar.reader.OrderedSourceNeighborReader;
import org.apache.graphar.storage.s3.S3Storage;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/** Verifies a snapshot-pinned Iceberg export and strict GraphAr S3 neighbor reads end to end. */
public class S3IcebergTopologyExportEndToEndTest {
    private static final Schema INPUT_SCHEMA =
            new Schema(
                    List.of(
                            new Field("source", ColumnType.of(ColumnType.Kind.INT64), false),
                            new Field("destination", ColumnType.of(ColumnType.Kind.INT64), false)));

    @Test
    public void exportsAnIcebergSnapshotToS3AndReadsNeighborsWithPhysicalRanges() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize(
                "e2e",
                Map.of(CatalogProperties.WAREHOUSE_LOCATION, "s3://iceberg-source/warehouse"));
        catalog.createNamespace(Namespace.of("graph"));
        Table table =
                catalog.createTable(
                        TableIdentifier.of("graph", "edges"),
                        new org.apache.iceberg.Schema(
                                Types.NestedField.required(1, "source", Types.LongType.get()),
                                Types.NestedField.required(2, "destination", Types.LongType.get())),
                        PartitionSpec.unpartitioned());
        URI tableData = URI.create(table.location() + "/data/source-topology.parquet");
        writeIcebergDataFile(table, tableData);
        table.newAppend()
                .appendFile(
                        DataFiles.builder(table.spec())
                                .withPath(tableData.toString())
                                .withFormat(FileFormat.PARQUET)
                                .withFileSizeInBytes(
                                        new IcebergFileIOStorage(table.io())
                                                .inputFile(tableData)
                                                .size())
                                .withRecordCount(1300)
                                .build())
                .commit();
        long snapshotId = table.currentSnapshot().snapshotId();

        InProcessS3 s3 = new InProcessS3();
        Path staging = Files.createTempDirectory("graphar-s3-e2e-");
        try {
            URI grapharRoot = URI.create("s3://graphar-export/topology/");
            GraphInfo graphInfo = graphInfoWithRelativeMetadataUris(grapharRoot);
            EdgeInfo edgeInfo = graphInfo.getEdgeInfo("person", "knows", "person");
            URI graphYaml = grapharRoot.resolve("ldbc_sample.graph.yml");
            URI manifest = grapharRoot.resolve("iceberg-export.yml");

            S3IcebergTopologyExport export = new S3IcebergTopologyExport(1300);
            assertEquals(
                    1300L,
                    export.exportOrderedSource(
                                    table,
                                    snapshotId,
                                    graphInfo,
                                    edgeInfo,
                                    4,
                                    "source",
                                    "destination",
                                    grapharRoot,
                                    graphYaml,
                                    manifest,
                                    s3.client(),
                                    staging,
                                    WriteMode.CREATE_NEW)
                            .edgeCount());

            List<Long> neighbors = new ArrayList<>();
            List<org.apache.graphar.io.ReadReport> reports;
            S3Storage storage = new S3Storage(s3.client(), staging);
            try (NeighborCursor cursor =
                    new OrderedSourceNeighborReader(
                                    edgeInfo, grapharRoot, new ParquetPhysicalReader(storage))
                            .neighbors(2)) {
                while (cursor.next()) {
                    neighbors.add(cursor.destination());
                }
                reports = cursor.reports();
            }

            assertEquals(1300, neighbors.size());
            assertEquals(10000L, neighbors.get(0).longValue());
            assertEquals(11299L, neighbors.get(neighbors.size() - 1).longValue());
            assertEquals(3, reports.size());
            assertEquals(EnumSet.noneOf(ReadCapability.class), reports.get(0).declined());
            for (int index = 1; index < reports.size(); index++) {
                assertTrue(reports.get(index).applied().contains(ReadCapability.ROW_RANGE));
                assertTrue(reports.get(index).declined().isEmpty());
            }
            assertTrue("Expected actual S3 byte-range reads", s3.rangeRequestCount > 0);
            assertTrue(s3.contains(manifest));
            assertTrue(s3.contains(graphYaml));
        } finally {
            deleteTree(staging);
            catalog.close();
        }
    }

    private static void writeIcebergDataFile(Table table, URI dataUri) throws IOException {
        IcebergFileIOStorage storage = new IcebergFileIOStorage(table.io());
        List<Row> rows = new ArrayList<>();
        for (long destination = 10000; destination < 11300; destination++) {
            rows.add(new Row(2, destination));
        }
        new ParquetPhysicalWriter(storage)
                .write(
                        new WriteRequest(dataUri, INPUT_SCHEMA, WriteMode.CREATE_NEW),
                        new SingleBatchCursor(new Rows(INPUT_SCHEMA, rows)));
    }

    private static Path fixturePath() {
        return Path.of("..", "..", "testing", "ldbc_sample", "parquet");
    }

    private static GraphInfo graphInfoWithRelativeMetadataUris(URI grapharRoot) throws IOException {
        GraphInfo fixture =
                new LocalFileSystemStringGraphInfoLoader()
                        .loadGraphInfo(fixturePath().resolve("ldbc_sample.graph.yml").toUri());
        return new GraphInfo(
                "ldbc_sample",
                Map.of(URI.create("person.vertex.yml"), fixture.getVertexInfo("person")),
                Map.of(
                        URI.create("person_knows_person.edge.yml"),
                        fixture.getEdgeInfo("person", "knows", "person")),
                grapharRoot,
                "gar/v1");
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            for (Path path :
                    paths.sorted(Comparator.reverseOrder())
                            .collect(java.util.stream.Collectors.toList())) {
                Files.delete(path);
            }
        }
    }

    private static final class Row implements org.apache.graphar.io.Row {
        private final Object[] values;

        private Row(long source, long destination) {
            this.values = new Object[] {source, destination};
        }

        @Override
        public Object value(int index) {
            return values[index];
        }
    }

    private static final class Rows implements RecordBatch {
        private final Schema schema;
        private final List<Row> rows;

        private Rows(Schema schema, List<Row> rows) {
            this.schema = schema;
            this.rows = rows;
        }

        @Override
        public Schema schema() {
            return schema;
        }

        @Override
        public int rowCount() {
            return rows.size();
        }

        @Override
        public org.apache.graphar.io.Row row(int index) {
            return rows.get(index);
        }
    }

    private static final class SingleBatchCursor implements BatchCursor {
        private final RecordBatch batch;
        private boolean read;

        private SingleBatchCursor(RecordBatch batch) {
            this.batch = batch;
        }

        @Override
        public boolean next() {
            if (read) {
                return false;
            }
            read = true;
            return true;
        }

        @Override
        public RecordBatch batch() {
            if (!read) {
                throw new IllegalStateException("No current batch. Call next() before batch().");
            }
            return batch;
        }

        @Override
        public void close() {}
    }

    private static final class InProcessS3 {
        private final Map<String, byte[]> objects = new HashMap<>();
        private int rangeRequestCount;

        private S3Client client() {
            return (S3Client)
                    Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {S3Client.class},
                            (proxy, method, arguments) -> {
                                if ("headObject".equals(method.getName())) {
                                    HeadObjectRequest request = (HeadObjectRequest) arguments[0];
                                    byte[] bytes =
                                            objects.get(key(request.bucket(), request.key()));
                                    if (bytes == null) {
                                        throw NoSuchKeyException.builder()
                                                .message("missing object")
                                                .build();
                                    }
                                    return HeadObjectResponse.builder()
                                            .contentLength((long) bytes.length)
                                            .versionId("test-version")
                                            .eTag("test-etag")
                                            .build();
                                }
                                if ("getObject".equals(method.getName())) {
                                    GetObjectRequest request = (GetObjectRequest) arguments[0];
                                    byte[] bytes = require(request.bucket(), request.key());
                                    String[] bounds =
                                            request.range().substring("bytes=".length()).split("-");
                                    int start = Integer.parseInt(bounds[0]);
                                    int end = Integer.parseInt(bounds[1]);
                                    rangeRequestCount++;
                                    return ResponseBytes.fromByteArray(
                                            GetObjectResponse.builder().build(),
                                            Arrays.copyOfRange(bytes, start, end + 1));
                                }
                                if ("putObject".equals(method.getName())) {
                                    PutObjectRequest request = (PutObjectRequest) arguments[0];
                                    String key = key(request.bucket(), request.key());
                                    if ("*".equals(request.ifNoneMatch())
                                            && objects.containsKey(key)) {
                                        throw new IllegalStateException(
                                                "Object already exists: " + key);
                                    }
                                    RequestBody body = (RequestBody) arguments[1];
                                    try (java.io.InputStream input =
                                            body.contentStreamProvider().newStream()) {
                                        objects.put(key, input.readAllBytes());
                                    }
                                    return PutObjectResponse.builder().build();
                                }
                                if ("serviceName".equals(method.getName())) {
                                    return "s3";
                                }
                                if ("close".equals(method.getName())) {
                                    return null;
                                }
                                throw new UnsupportedOperationException(method.toString());
                            });
        }

        private boolean contains(URI uri) {
            return objects.containsKey(key(uri.getHost(), uri.getPath().substring(1)));
        }

        private byte[] require(String bucket, String objectKey) {
            byte[] bytes = objects.get(key(bucket, objectKey));
            if (bytes == null) {
                throw NoSuchKeyException.builder().message("missing object").build();
            }
            return bytes;
        }

        private static String key(String bucket, String objectKey) {
            return bucket + "/" + objectKey;
        }
    }
}
