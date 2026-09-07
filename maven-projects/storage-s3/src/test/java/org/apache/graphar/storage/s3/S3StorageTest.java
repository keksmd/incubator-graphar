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

import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.graphar.storage.PositionOutput;
import org.apache.graphar.storage.SeekableInput;
import org.junit.Assert;
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

/** Credential-free contract tests using an injected in-memory S3 SDK transport. */
public class S3StorageTest {
    @Test
    public void stagesOutputAndUsesVersionPinnedByteRanges() throws Exception {
        byte[] object = "abcdef".getBytes();
        RequestLog log = new RequestLog(object);
        S3Storage storage =
                new S3Storage(log.client(), Files.createTempDirectory("graphar-s3-stage"));
        URI uri = URI.create("s3://bucket/graph/a.parquet");

        Assert.assertTrue(storage.exists(uri));
        try (SeekableInput input = storage.inputFile(uri).open()) {
            input.seek(2);
            ByteBuffer bytes = ByteBuffer.allocate(3);
            Assert.assertEquals(3, input.read(bytes));
            Assert.assertArrayEquals("cde".getBytes(), bytes.array());
        }
        Assert.assertEquals("bytes=2-4", log.range);
        Assert.assertEquals("version-1", log.versionId);

        URI target = URI.create("s3://bucket/graph/new.bin");
        try (PositionOutput output = storage.outputFile(target).create()) {
            output.write(new byte[] {9, 8, 7});
        }
        Assert.assertEquals("*", log.ifNoneMatch);
        Assert.assertArrayEquals(new byte[] {9, 8, 7}, log.putBytes);
    }

    @Test
    public void rejectsAnExistingObjectBeforeStagingAnyByte() throws Exception {
        RequestLog log = new RequestLog("abcdef".getBytes());
        S3Storage storage =
                new S3Storage(log.client(), Files.createTempDirectory("graphar-s3-stage"));

        try {
            storage.outputFile(URI.create("s3://bucket/graph/a.parquet")).create();
            Assert.fail("create() must reject an object that already exists.");
        } catch (FileAlreadyExistsException expected) {
            Assert.assertNull("No object may be published on a rejected create().", log.putBytes);
        }
    }

    private static final class RequestLog {
        private final byte[] object;
        private String range;
        private String versionId;
        private String ifNoneMatch;
        private byte[] putBytes;

        private RequestLog(byte[] object) {
            this.object = object;
        }

        private S3Client client() {
            return (S3Client)
                    Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[] {S3Client.class},
                            (proxy, method, arguments) -> {
                                if ("headObject".equals(method.getName())) {
                                    HeadObjectRequest request = (HeadObjectRequest) arguments[0];
                                    Assert.assertEquals("bucket", request.bucket());
                                    if (!"graph/a.parquet".equals(request.key())) {
                                        throw NoSuchKeyException.builder().statusCode(404).build();
                                    }
                                    return HeadObjectResponse.builder()
                                            .contentLength((long) object.length)
                                            .versionId("version-1")
                                            .eTag("etag-1")
                                            .build();
                                }
                                if ("getObject".equals(method.getName())) {
                                    GetObjectRequest request = (GetObjectRequest) arguments[0];
                                    range = request.range();
                                    versionId = request.versionId();
                                    String[] bounds = range.substring("bytes=".length()).split("-");
                                    int start = Integer.parseInt(bounds[0]);
                                    int end = Integer.parseInt(bounds[1]);
                                    return ResponseBytes.fromByteArray(
                                            GetObjectResponse.builder().build(),
                                            Arrays.copyOfRange(object, start, end + 1));
                                }
                                if ("putObject".equals(method.getName())) {
                                    PutObjectRequest request = (PutObjectRequest) arguments[0];
                                    ifNoneMatch = request.ifNoneMatch();
                                    RequestBody body = (RequestBody) arguments[1];
                                    putBytes =
                                            body.contentStreamProvider().newStream().readAllBytes();
                                    return PutObjectResponse.builder().build();
                                }
                                if ("serviceName".equals(method.getName())) {
                                    return "s3";
                                }
                                throw new UnsupportedOperationException(method.toString());
                            });
        }
    }
}
