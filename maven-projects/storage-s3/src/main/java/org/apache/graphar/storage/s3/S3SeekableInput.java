/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

final class S3SeekableInput implements SeekableInput {
    /**
     * Parquet reads its footer, indexes and page headers in many small adjacent reads. Coalescing
     * them is essential for S3, where every range request otherwise becomes an HTTP round-trip.
     */
    private static final int READ_AHEAD_BYTES = 64 * 1024;

    private final S3Client client;
    private final S3Storage.Location location;
    private final long size;
    private final String versionId;
    private final String eTag;
    private long position;
    private long bufferStart = -1;
    private byte[] buffer = new byte[0];
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
        int copied = 0;
        while (destination.hasRemaining() && position < size) {
            int available = bufferedBytes();
            if (available == 0) {
                readAhead();
                available = bufferedBytes();
            }
            int count = Math.min(destination.remaining(), available);
            destination.put(buffer, (int) (position - bufferStart), count);
            position += count;
            copied += count;
        }
        return copied;
    }

    private int bufferedBytes() {
        if (position < bufferStart || position >= bufferStart + buffer.length) {
            return 0;
        }
        return (int) Math.min(bufferStart + buffer.length - position, Integer.MAX_VALUE);
    }

    private void readAhead() throws IOException {
        long endExclusive = position + Math.min(size - position, READ_AHEAD_BYTES);
        int count = (int) (endExclusive - position);
        GetObjectRequest.Builder request =
                GetObjectRequest.builder()
                        .bucket(location.bucket)
                        .key(location.key)
                        .range("bytes=" + position + "-" + (endExclusive - 1));
        if (versionId != null) {
            request.versionId(versionId);
        } else if (eTag != null) {
            request.ifMatch(eTag);
        }
        try {
            ResponseBytes<GetObjectResponse> response =
                    client.getObject(request.build(), ResponseTransformer.toBytes());
            byte[] bytes = response.asByteArray();
            if (bytes.length != count) {
                throw new IOException(
                        "S3 returned " + bytes.length + " bytes for requested range of " + count);
            }
            bufferStart = position;
            buffer = bytes;
        } catch (IOException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new IOException("Cannot range-read S3 object " + location.uri, exception);
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    private void requireOpen() throws IOException {
        if (closed) {
            throw new IOException("S3 input is closed.");
        }
    }
}
