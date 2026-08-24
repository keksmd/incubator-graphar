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

package org.apache.graphar.storage.s3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.graphar.storage.PositionOutput;
import org.apache.graphar.storage.SeekableInput;
import org.junit.Assume;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * Opt-in MinIO transport test. Run with {@code GRAPHAR_MINIO_ENDPOINT=http://localhost:19000}.
 *
 * <p>The default test lifecycle excludes {@code *IT}; this class intentionally requires a
 * separately started MinIO endpoint rather than adding Docker to normal SDK verification.
 */
public class MinioS3StorageIntegrationIT {
    private static final String ENDPOINT = "GRAPHAR_MINIO_ENDPOINT";
    private static final String ACCESS_KEY = "GRAPHAR_MINIO_ACCESS_KEY";
    private static final String SECRET_KEY = "GRAPHAR_MINIO_SECRET_KEY";

    @Test
    public void stagesConditionallyPublishesAndRangeReadsOverMinio() throws Exception {
        String endpoint = System.getenv(ENDPOINT);
        Assume.assumeTrue(
                "Set " + ENDPOINT + " to run the MinIO transport test.", endpoint != null);

        String bucket = "graphar-minio-" + UUID.randomUUID().toString().replace("-", "");
        Path staging = Files.createTempDirectory("graphar-minio-stage-");
        try (S3Client client = client(endpoint)) {
            client.createBucket(request -> request.bucket(bucket));
            S3Storage storage = new S3Storage(client, staging);
            URI object = URI.create("s3://" + bucket + "/topology/chunk.parquet");

            assertFalse(storage.exists(object));
            write(storage.outputFile(object).create(), new byte[] {0, 1, 2, 3, 4, 5});
            assertTrue(storage.exists(object));
            assertEquals(6L, storage.inputFile(object).size());

            ByteBuffer range = ByteBuffer.allocateDirect(3);
            try (SeekableInput input = storage.inputFile(object).open()) {
                input.seek(2);
                assertEquals(3, input.read(range));
                assertEquals(5L, input.position());
                input.seek(6);
                assertEquals(-1, input.read(ByteBuffer.allocate(1)));
            }
            range.flip();
            byte[] actual = new byte[range.remaining()];
            range.get(actual);
            assertArrayEquals(new byte[] {2, 3, 4}, actual);

            try {
                write(storage.outputFile(object).create(), new byte[] {9});
                fail("create() must not overwrite an existing S3 object.");
            } catch (IOException expected) {
                // MinIO returns the S3 conditional-publication failure through the AWS SDK.
            }
            write(storage.outputFile(object).createOrOverwrite(), new byte[] {9});
            try (SeekableInput input = storage.inputFile(object).open()) {
                ByteBuffer replacement = ByteBuffer.allocate(1);
                assertEquals(1, input.read(replacement));
                assertArrayEquals(new byte[] {9}, replacement.array());
            }
        } finally {
            deleteTree(staging);
        }
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

    private static void write(PositionOutput output, byte[] bytes) throws IOException {
        try (PositionOutput ignored = output) {
            output.write(bytes);
        }
    }

    private static void deleteTree(Path root) throws IOException {
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
