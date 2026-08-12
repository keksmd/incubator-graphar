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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.PositionOutput;
import org.apache.graphar.storage.SeekableInput;
import org.apache.graphar.storage.Storage;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

/** Adapts the caller-owned Iceberg {@link FileIO} into GraphAr physical storage. */
public final class IcebergFileIOStorage implements Storage {
    private final FileIO fileIO;

    public IcebergFileIOStorage(FileIO fileIO) {
        this.fileIO = Objects.requireNonNull(fileIO, "Iceberg FileIO cannot be null.");
    }

    @Override
    public InputFile inputFile(URI uri) {
        Objects.requireNonNull(uri, "URI cannot be null.");
        org.apache.iceberg.io.InputFile input = fileIO.newInputFile(uri.toString());
        return new InputFile() {
            @Override
            public URI uri() {
                return uri;
            }

            @Override
            public long size() {
                return input.getLength();
            }

            @Override
            public SeekableInput open() {
                return new IcebergSeekableInput(input.newStream());
            }
        };
    }

    @Override
    public OutputFile outputFile(URI uri) {
        Objects.requireNonNull(uri, "URI cannot be null.");
        org.apache.iceberg.io.OutputFile output = fileIO.newOutputFile(uri.toString());
        return new OutputFile() {
            @Override
            public URI uri() {
                return uri;
            }

            @Override
            public PositionOutput create() {
                return new IcebergPositionOutput(output.create());
            }

            @Override
            public PositionOutput createOrOverwrite() {
                return new IcebergPositionOutput(output.createOrOverwrite());
            }
        };
    }

    @Override
    public boolean exists(URI uri) {
        return fileIO.newInputFile(uri.toString()).exists();
    }

    private static final class IcebergSeekableInput implements SeekableInput {
        private final SeekableInputStream input;

        private IcebergSeekableInput(SeekableInputStream input) {
            this.input = input;
        }

        @Override
        public long position() throws IOException {
            return input.getPos();
        }

        @Override
        public void seek(long newPosition) throws IOException {
            input.seek(newPosition);
        }

        @Override
        public int read(ByteBuffer destination) throws IOException {
            if (!destination.hasRemaining()) {
                return 0;
            }
            byte[] bytes = new byte[Math.min(destination.remaining(), 8192)];
            int count = input.read(bytes);
            if (count > 0) {
                destination.put(bytes, 0, count);
            }
            return count;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static final class IcebergPositionOutput implements PositionOutput {
        private final PositionOutputStream output;

        private IcebergPositionOutput(PositionOutputStream output) {
            this.output = output;
        }

        @Override
        public long position() throws IOException {
            return output.getPos();
        }

        @Override
        public void write(ByteBuffer source) throws IOException {
            byte[] bytes = new byte[source.remaining()];
            source.get(bytes);
            output.write(bytes);
        }

        @Override
        public void write(byte[] source, int offset, int length) throws IOException {
            output.write(source, offset, length);
        }

        @Override
        public void flush() throws IOException {
            output.flush();
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }
}
