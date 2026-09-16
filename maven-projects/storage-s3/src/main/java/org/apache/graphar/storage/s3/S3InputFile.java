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
