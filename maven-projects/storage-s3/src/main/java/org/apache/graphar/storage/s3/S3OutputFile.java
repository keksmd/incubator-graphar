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

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.PositionOutput;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

final class S3OutputFile implements OutputFile {
    private final S3Client client;
    private final S3Storage.Location location;
    private final Path stagingDirectory;

    S3OutputFile(S3Client client, S3Storage.Location location, Path stagingDirectory) {
        this.client = client;
        this.location = location;
        this.stagingDirectory = stagingDirectory;
    }

    @Override
    public URI uri() {
        return location.uri;
    }

    @Override
    public PositionOutput create() throws IOException {
        failIfPresent();
        return open(true);
    }

    /**
     * Rejects an existing object before any byte is staged, so that a caller learns about the
     * conflict at open time the way the local adapter does. A head request that cannot answer is
     * not treated as a conflict: the conditional publication in {@link StagedOutput#close()}
     * remains the atomic guarantee.
     *
     * @throws FileAlreadyExistsException when the object is already present
     */
    private void failIfPresent() throws IOException {
        try {
            client.headObject(
                    HeadObjectRequest.builder().bucket(location.bucket).key(location.key).build());
        } catch (RuntimeException absentOrUnknown) {
            return;
        }
        throw new FileAlreadyExistsException(location.uri.toString());
    }

    @Override
    public PositionOutput createOrOverwrite() throws IOException {
        return open(false);
    }

    private PositionOutput open(boolean createOnly) throws IOException {
        Path stage = Files.createTempFile(stagingDirectory, "graphar-s3-", ".stage");
        try {
            return new StagedOutput(client, location, stage, createOnly);
        } catch (IOException | RuntimeException exception) {
            Files.deleteIfExists(stage);
            throw exception;
        }
    }

    private static final class StagedOutput implements PositionOutput {
        private final S3Client client;
        private final S3Storage.Location location;
        private final Path stage;
        private final boolean createOnly;
        private final OutputStream output;
        private long position;
        private boolean closed;

        private StagedOutput(
                S3Client client, S3Storage.Location location, Path stage, boolean createOnly)
                throws IOException {
            this.client = client;
            this.location = location;
            this.stage = stage;
            this.createOnly = createOnly;
            this.output = Files.newOutputStream(stage);
        }

        @Override
        public long position() throws IOException {
            requireOpen();
            return position;
        }

        @Override
        public void write(ByteBuffer source) throws IOException {
            requireOpen();
            byte[] bytes = new byte[source.remaining()];
            source.get(bytes);
            write(bytes, 0, bytes.length);
        }

        @Override
        public void write(byte[] source, int offset, int length) throws IOException {
            requireOpen();
            output.write(source, offset, length);
            position = Math.addExact(position, length);
        }

        @Override
        public void flush() throws IOException {
            requireOpen();
            output.flush();
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try {
                output.close();
                PutObjectRequest.Builder request =
                        PutObjectRequest.builder().bucket(location.bucket).key(location.key);
                if (createOnly) {
                    request.ifNoneMatch("*");
                }
                client.putObject(request.build(), RequestBody.fromFile(stage));
            } catch (RuntimeException exception) {
                throw new IOException("Cannot publish S3 object " + location.uri, exception);
            } finally {
                Files.deleteIfExists(stage);
            }
        }

        private void requireOpen() throws IOException {
            if (closed) {
                throw new IOException("S3 output is closed.");
            }
        }
    }
}
