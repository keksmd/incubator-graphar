/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0. */
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
    private final S3Client client;
    private final S3Storage.Location location;
    private final long size;
    private final String versionId;
    private final String eTag;
    private long position;
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
        int count = (int) Math.min(destination.remaining(), size - position);
        GetObjectRequest.Builder request =
                GetObjectRequest.builder()
                        .bucket(location.bucket)
                        .key(location.key)
                        .range("bytes=" + position + "-" + (position + count - 1));
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
            destination.put(bytes);
            position += bytes.length;
            return bytes.length;
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
