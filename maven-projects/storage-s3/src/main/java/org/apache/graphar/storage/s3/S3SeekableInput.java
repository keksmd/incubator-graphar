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
import java.nio.ByteBuffer;
import org.apache.graphar.storage.SeekableInput;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Reads an object through range requests, keeping the last fetched block in memory so that the
 * small sequential reads typical of columnar footers do not cost one round trip each. A read at
 * least as large as the block size bypasses the buffer and is served by a single request.
 */
final class S3SeekableInput implements SeekableInput {
    private static final int BLOCK_SIZE = 1 << 20;
    private static final byte[] NO_BLOCK = new byte[0];

    private final S3Client client;
    private final S3Storage.Location location;
    private final long size;
    private final String versionId;
    private final String eTag;
    private long position;
    private byte[] block = NO_BLOCK;
    private long blockStart;
    private int blockLength;
    private boolean closed;

    S3SeekableInput(
            S3Client client,
            S3Storage.Location location,
            long size,
            String versionId,
            String eTag) {
        this.client = client;
        this.location = location;
        this.size = size;
        this.versionId = versionId;
        this.eTag = eTag;
    }

    @Override
    public long position() throws IOException {
        requireOpen();
        return position;
    }

    @Override
    public void seek(long newPosition) throws IOException {
        requireOpen();
        if (newPosition < 0) {
            throw new IllegalArgumentException("S3 seek position must be non-negative.");
        }
        position = newPosition;
    }

    @Override
    public int read(ByteBuffer destination) throws IOException {
        requireOpen();
        if (!destination.hasRemaining()) {
            return 0;
        }
        if (position >= size) {
            return -1;
        }
        int wanted = (int) Math.min(destination.remaining(), size - position);
        if (wanted >= BLOCK_SIZE) {
            byte[] bytes = fetch(position, wanted);
            destination.put(bytes);
            position += bytes.length;
            return bytes.length;
        }
        if (position < blockStart || position >= blockStart + blockLength) {
            int length = (int) Math.min(BLOCK_SIZE, size - position);
            block = fetch(position, length);
            blockStart = position;
            blockLength = block.length;
        }
        int offset = (int) (position - blockStart);
        int count = Math.min(wanted, blockLength - offset);
        destination.put(block, offset, count);
        position += count;
        return count;
    }

    private byte[] fetch(long start, int length) throws IOException {
        GetObjectRequest.Builder request =
                GetObjectRequest.builder()
                        .bucket(location.bucket)
                        .key(location.key)
                        .range("bytes=" + start + "-" + (start + length - 1));
        if (versionId != null) {
            request.versionId(versionId);
        } else if (eTag != null) {
            request.ifMatch(eTag);
        }
        try {
            ResponseBytes<GetObjectResponse> response =
                    client.getObject(request.build(), ResponseTransformer.toBytes());
            byte[] bytes = response.asByteArray();
            if (bytes.length != length) {
                throw new IOException(
                        "S3 returned " + bytes.length + " bytes for requested range of " + length);
            }
            return bytes;
        } catch (IOException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new IOException("Cannot range-read S3 object " + location.uri, exception);
        }
    }

    @Override
    public void close() {
        closed = true;
        block = NO_BLOCK;
        blockLength = 0;
    }

    private void requireOpen() throws IOException {
        if (closed) {
            throw new IOException("S3 input is closed.");
        }
    }
}
