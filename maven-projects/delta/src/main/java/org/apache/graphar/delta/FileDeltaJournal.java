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

package org.apache.graphar.delta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.zip.CRC32;
import org.apache.graphar.reader.CsrDirection;

/**
 * A {@link DeltaJournal} kept in one append-only file.
 *
 * <p>A crash can only ever cut the log at an arbitrary byte, so a record is accepted on replay only
 * when its own checksum matches. Opening the file finds the last record that does and truncates
 * what follows: a half-written tail is data that was never made durable, and keeping it would mean
 * appending after a hole. Everything before it is intact, which is what makes the surviving delta
 * usable rather than merely detectable as broken.
 *
 * <p>The header pins the log to one base vertex space. Replaying a log against a different base
 * would hand out vertex numbers that mean something else, so the mismatch is refused at open rather
 * than discovered as wrong answers.
 */
public final class FileDeltaJournal implements DeltaJournal {
    private static final byte[] MAGIC = "GARDELTA".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = MAGIC.length + Integer.BYTES * 3 + Long.BYTES * 2;
    private static final byte VERTEX_RECORD = 1;
    private static final byte EDGE_RECORD = 2;
    private static final int READ_BUFFER_BYTES = 1 << 16;
    private static final int MAX_IDENTIFIER_BYTES = 1 << 20;

    private final Path path;
    private final long baseVertexCount;
    private final CsrDirection direction;
    private final long edgeFloor;
    private final ByteBuffer record = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
    private FileChannel channel;

    private FileDeltaJournal(
            Path path,
            long baseVertexCount,
            CsrDirection direction,
            long edgeFloor,
            FileChannel channel) {
        this.path = path;
        this.baseVertexCount = baseVertexCount;
        this.direction = direction;
        this.edgeFloor = edgeFloor;
        this.channel = channel;
    }

    /**
     * Opens the journal at {@code path}, creating it when it does not exist and refusing it when it
     * belongs to a different base vertex space or adjacency direction.
     */
    public static FileDeltaJournal open(Path path, long baseVertexCount, CsrDirection direction)
            throws IOException {
        Objects.requireNonNull(path, "Journal path cannot be null.");
        Objects.requireNonNull(direction, "CSR direction cannot be null.");
        if (!Files.exists(path)) {
            writeHeader(path, baseVertexCount, direction, 0L);
            return new FileDeltaJournal(path, baseVertexCount, direction, 0L, appendChannel(path));
        }
        Header header = readHeader(path);
        if (header.baseVertexCount != baseVertexCount) {
            throw new DeltaJournalFormatException(
                    "Journal "
                            + path
                            + " belongs to a base of "
                            + header.baseVertexCount
                            + " vertices, not "
                            + baseVertexCount
                            + ".");
        }
        if (header.direction != direction) {
            throw new DeltaJournalFormatException(
                    "Journal "
                            + path
                            + " belongs to a "
                            + header.direction
                            + " projection, not "
                            + direction
                            + ".");
        }
        long intact = intactLength(path);
        try (FileChannel truncating =
                FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            if (truncating.size() > intact) {
                truncating.truncate(intact);
                truncating.force(true);
            }
        }
        return new FileDeltaJournal(
                path, baseVertexCount, direction, header.edgeFloor, appendChannel(path));
    }

    @Override
    public long edgeFloor() {
        return edgeFloor;
    }

    @Override
    public void vertex(int typeOrdinal, String externalId) throws IOException {
        byte[] identifier = externalId.getBytes(StandardCharsets.UTF_8);
        if (identifier.length > MAX_IDENTIFIER_BYTES) {
            throw new IllegalArgumentException(
                    "Vertex identifier is longer than the journal accepts: " + identifier.length);
        }
        ByteBuffer payload =
                ByteBuffer.allocate(1 + Integer.BYTES * 2 + identifier.length + Integer.BYTES)
                        .order(ByteOrder.LITTLE_ENDIAN);
        payload.put(VERTEX_RECORD);
        payload.putInt(typeOrdinal);
        payload.putInt(identifier.length);
        payload.put(identifier);
        payload.putInt(0);
        appendRecord(payload);
    }

    @Override
    public void edge(int source, int target) throws IOException {
        record.clear();
        record.put(EDGE_RECORD);
        record.putInt(source);
        record.putInt(target);
        record.putInt(0);
        appendRecord(record);
    }

    @Override
    public void sync() throws IOException {
        channel.force(false);
    }

    @Override
    public void replay(Visitor visitor) throws IOException {
        Objects.requireNonNull(visitor, "Journal visitor cannot be null.");
        try (FileChannel reading = FileChannel.open(path, StandardOpenOption.READ)) {
            reading.position(HEADER_BYTES);
            RecordReader reader = new RecordReader(reading);
            while (reader.next()) {
                if (reader.type == VERTEX_RECORD) {
                    visitor.vertex(reader.typeOrdinal, reader.identifier);
                } else {
                    visitor.edge(reader.source, reader.target);
                }
            }
        }
    }

    @Override
    public void rewrite(long baseVertexCount, long edgeFloor, Content content) throws IOException {
        Objects.requireNonNull(content, "Journal content cannot be null.");
        Path staging = path.resolveSibling(path.getFileName() + ".rewrite");
        writeHeader(staging, baseVertexCount, direction, edgeFloor);
        try (FileDeltaJournal staged =
                new FileDeltaJournal(
                        staging, baseVertexCount, direction, edgeFloor, appendChannel(staging))) {
            content.writeTo(staged);
            staged.sync();
        }
        channel.close();
        Files.move(staging, path, StandardCopyOption.REPLACE_EXISTING);
        channel = appendChannel(path);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private void appendRecord(ByteBuffer payload) throws IOException {
        int end = payload.position();
        CRC32 checksum = new CRC32();
        checksum.update(payload.array(), 0, end - Integer.BYTES);
        payload.position(end - Integer.BYTES);
        payload.putInt((int) checksum.getValue());
        payload.position(0);
        payload.limit(end);
        while (payload.hasRemaining()) {
            channel.write(payload);
        }
    }

    private static FileChannel appendChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    private static void writeHeader(
            Path path, long baseVertexCount, CsrDirection direction, long edgeFloor)
            throws IOException {
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        header.put(MAGIC);
        header.putInt(VERSION);
        header.putLong(baseVertexCount);
        header.putInt(direction.ordinal());
        header.putLong(edgeFloor);
        CRC32 checksum = new CRC32();
        checksum.update(header.array(), 0, HEADER_BYTES - Integer.BYTES);
        header.putInt((int) checksum.getValue());
        header.flip();
        try (FileChannel creating =
                FileChannel.open(
                        path,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING)) {
            while (header.hasRemaining()) {
                creating.write(header);
            }
            creating.force(true);
        }
    }

    private static Header readHeader(Path path) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        try (FileChannel reading = FileChannel.open(path, StandardOpenOption.READ)) {
            while (buffer.hasRemaining()) {
                if (reading.read(buffer) < 0) {
                    throw new DeltaJournalFormatException(
                            "Journal " + path + " is shorter than its header.");
                }
            }
        }
        buffer.flip();
        byte[] magic = new byte[MAGIC.length];
        buffer.get(magic);
        for (int index = 0; index < MAGIC.length; index++) {
            if (magic[index] != MAGIC[index]) {
                throw new DeltaJournalFormatException("File " + path + " is not a delta journal.");
            }
        }
        int version = buffer.getInt();
        if (version != VERSION) {
            throw new DeltaJournalFormatException(
                    "Journal " + path + " is of version " + version + ", not " + VERSION + ".");
        }
        long baseVertexCount = buffer.getLong();
        int directionOrdinal = buffer.getInt();
        long edgeFloor = buffer.getLong();
        int stored = buffer.getInt();
        CRC32 checksum = new CRC32();
        checksum.update(buffer.array(), 0, HEADER_BYTES - Integer.BYTES);
        if (stored != (int) checksum.getValue()) {
            throw new DeltaJournalFormatException("Journal " + path + " has a damaged header.");
        }
        CsrDirection[] directions = CsrDirection.values();
        if (directionOrdinal < 0 || directionOrdinal >= directions.length) {
            throw new DeltaJournalFormatException(
                    "Journal " + path + " names an unknown direction: " + directionOrdinal + ".");
        }
        return new Header(baseVertexCount, directions[directionOrdinal], edgeFloor);
    }

    private static long intactLength(Path path) throws IOException {
        try (FileChannel reading = FileChannel.open(path, StandardOpenOption.READ)) {
            reading.position(HEADER_BYTES);
            RecordReader reader = new RecordReader(reading);
            while (reader.next()) {
                continue;
            }
            return HEADER_BYTES + reader.consumed;
        }
    }

    private static final class Header {
        private final long baseVertexCount;
        private final CsrDirection direction;
        private final long edgeFloor;

        private Header(long baseVertexCount, CsrDirection direction, long edgeFloor) {
            this.baseVertexCount = baseVertexCount;
            this.direction = direction;
            this.edgeFloor = edgeFloor;
        }
    }

    /**
     * Reads records until one fails its checksum or is cut short, which is where a crashed writer
     * stopped.
     */
    private static final class RecordReader {
        private final FileChannel channel;
        private final ByteBuffer buffer =
                ByteBuffer.allocate(READ_BUFFER_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        private final CRC32 checksum = new CRC32();

        private long consumed;
        private byte type;
        private int typeOrdinal;
        private String identifier;
        private int source;
        private int target;

        private RecordReader(FileChannel channel) {
            this.channel = channel;
            buffer.limit(0);
        }

        private boolean next() throws IOException {
            if (!ensure(1)) {
                return false;
            }
            int start = buffer.position();
            byte recordType = buffer.get(start);
            if (recordType == EDGE_RECORD) {
                return readEdge(start);
            }
            if (recordType == VERTEX_RECORD) {
                return readVertex(start);
            }
            return false;
        }

        private boolean readEdge(int start) throws IOException {
            int length = 1 + Integer.BYTES * 2;
            if (!ensure(length + Integer.BYTES)) {
                return false;
            }
            start = buffer.position();
            if (!matches(start, length)) {
                return false;
            }
            type = EDGE_RECORD;
            source = buffer.getInt(start + 1);
            target = buffer.getInt(start + 1 + Integer.BYTES);
            advance(length + Integer.BYTES);
            return true;
        }

        private boolean readVertex(int start) throws IOException {
            if (!ensure(1 + Integer.BYTES * 2)) {
                return false;
            }
            start = buffer.position();
            int identifierBytes = buffer.getInt(start + 1 + Integer.BYTES);
            if (identifierBytes < 0 || identifierBytes > MAX_IDENTIFIER_BYTES) {
                return false;
            }
            int length = 1 + Integer.BYTES * 2 + identifierBytes;
            if (!ensure(length + Integer.BYTES)) {
                return false;
            }
            start = buffer.position();
            if (!matches(start, length)) {
                return false;
            }
            type = VERTEX_RECORD;
            typeOrdinal = buffer.getInt(start + 1);
            identifier =
                    new String(
                            buffer.array(),
                            start + 1 + Integer.BYTES * 2,
                            identifierBytes,
                            StandardCharsets.UTF_8);
            advance(length + Integer.BYTES);
            return true;
        }

        private boolean matches(int start, int length) {
            checksum.reset();
            checksum.update(buffer.array(), start, length);
            return buffer.getInt(start + length) == (int) checksum.getValue();
        }

        private void advance(int bytes) {
            buffer.position(buffer.position() + bytes);
            consumed += bytes;
        }

        private boolean ensure(int bytes) throws IOException {
            if (buffer.remaining() >= bytes) {
                return true;
            }
            buffer.compact();
            while (buffer.position() < bytes) {
                if (channel.read(buffer) < 0) {
                    break;
                }
            }
            buffer.flip();
            return buffer.remaining() >= bytes;
        }
    }
}
