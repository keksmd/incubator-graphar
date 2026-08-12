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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.Storage;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** S3-backed GraphAr storage using an injected AWS SDK client. */
public final class S3Storage implements Storage {
    private final S3Client client;
    private final Path stagingDirectory;

    /** Creates S3 storage. The caller owns the client and is responsible for closing it. */
    public S3Storage(S3Client client, Path stagingDirectory) throws IOException {
        this.client = Objects.requireNonNull(client, "S3 client cannot be null.");
        this.stagingDirectory =
                Objects.requireNonNull(stagingDirectory, "Staging directory cannot be null.");
        Files.createDirectories(stagingDirectory);
        if (!Files.isDirectory(stagingDirectory)) {
            throw new IllegalArgumentException(
                    "S3 staging path is not a directory: " + stagingDirectory);
        }
    }

    @Override
    public InputFile inputFile(URI uri) {
        Location location = Location.from(uri);
        return new S3InputFile(client, location);
    }

    @Override
    public OutputFile outputFile(URI uri) {
        Location location = Location.from(uri);
        return new S3OutputFile(client, location, stagingDirectory);
    }

    @Override
    public boolean exists(URI uri) throws IOException {
        Location location = Location.from(uri);
        try {
            client.headObject(
                    HeadObjectRequest.builder().bucket(location.bucket).key(location.key).build());
            return true;
        } catch (NoSuchKeyException exception) {
            return false;
        } catch (S3Exception exception) {
            if (exception.statusCode() == 404) {
                return false;
            }
            throw new IOException("Cannot inspect S3 object " + uri, exception);
        } catch (RuntimeException exception) {
            throw new IOException("Cannot inspect S3 object " + uri, exception);
        }
    }

    static final class Location {
        final URI uri;
        final String bucket;
        final String key;

        private Location(URI uri, String bucket, String key) {
            this.uri = uri;
            this.bucket = bucket;
            this.key = key;
        }

        private static Location from(URI uri) {
            if (uri == null
                    || !"s3".equalsIgnoreCase(uri.getScheme())
                    || uri.getHost() == null
                    || uri.getHost().isEmpty()
                    || uri.getPath() == null
                    || uri.getPath().length() <= 1
                    || uri.getUserInfo() != null
                    || uri.getQuery() != null
                    || uri.getFragment() != null) {
                throw new IllegalArgumentException(
                        "Expected an absolute s3://bucket/key URI: " + uri);
            }
            return new Location(uri, uri.getHost(), uri.getPath().substring(1));
        }
    }
}
