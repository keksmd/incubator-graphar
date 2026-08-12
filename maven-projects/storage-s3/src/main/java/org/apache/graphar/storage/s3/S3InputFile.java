/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0. */
package org.apache.graphar.storage.s3;

import java.io.IOException;
import java.net.URI;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.SeekableInput;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

final class S3InputFile implements InputFile {
    private final S3Client client;
    private final S3Storage.Location location;

    S3InputFile(S3Client client, S3Storage.Location location) {
        this.client = client;
        this.location = location;
    }

    @Override
    public URI uri() {
        return location.uri;
    }

    @Override
    public long size() throws IOException {
        return head().contentLength();
    }

    @Override
    public SeekableInput open() throws IOException {
        HeadObjectResponse response = head();
        return new S3SeekableInput(
                client, location, response.contentLength(), response.versionId(), response.eTag());
    }

    private HeadObjectResponse head() throws IOException {
        try {
            return client.headObject(
                    HeadObjectRequest.builder().bucket(location.bucket).key(location.key).build());
        } catch (RuntimeException exception) {
            throw new IOException("Cannot inspect S3 object " + location.uri, exception);
        }
    }
}
