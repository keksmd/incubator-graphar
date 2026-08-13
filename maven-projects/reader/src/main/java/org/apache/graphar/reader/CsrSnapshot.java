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

package org.apache.graphar.reader;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Objects;
import java.util.zip.CRC32;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.OutputFile;
import org.apache.graphar.storage.PositionOutput;
import org.apache.graphar.storage.SeekableInput;

/**
 * Writes a materialized CSR to storage and reads it back, so a replica can serve a projection it
 * did not build.
 *
 * <p>Building the projection is the whole of a replica's startup: every replica reads the same
 * immutable dataset and derives the same arrays from it, and every one of them pays for that
 * separately. The arrays are already the format they are served in, so one host can write them once
 * and the rest can load them at the speed of their storage.
 *
 * <p>The file is a header, the offsets, the destinations, and a CRC32 over everything before it.
 * Integers are little-endian, which is the order the arrays already have in memory on the hosts
 * this serves. The checksum is what separates a snapshot from a torn write: a replica that loaded a
 * truncated file would serve a graph with silently missing edges, so a snapshot that does not
 * checksum is rejected rather than served.
 *
 * <p>Only the topology is stored. A replica still builds its own identifier index, because that
 * index is derived from the vertex payload rather than from the projection.
 */
public final class CsrSnapshot {
    private static final long MAGIC = 0x47415243_53523031L;
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_BYTES = 8 + 4 + 8 + 8;
    private static final int TRANSFER_BYTES = 1 << 20;

    private CsrSnapshot() {}

    /** Returns the size the snapshot of a projection of this shape occupies in storage. */
    public static long snapshotBytes(long vertexCount, long entryCount) {
        return Math.addExact(
                HEADER_BYTES + 8L, ProjectionCapacity.heapBytes(vertexCount, entryCount));
    }

    /** Writes {@code csr} to {@code target}, replacing any snapshot already there. */
    public static void write(CsrGraph csr, OutputFile target) throws IOException {
        Objects.requireNonNull(csr, "CSR cannot be null.");
        Objects.requireNonNull(target, "Snapshot target cannot be null.");
        int[] offsets = csr.rawOffsets();
        int[] destinations = csr.rawDestinations();
        CRC32 checksum = new CRC32();
        try (PositionOutput output = target.createOrOverwrite()) {
            ByteBuffer header = allocate(HEADER_BYTES);
            header.putLong(MAGIC);
            header.putInt(FORMAT_VERSION);
            header.putLong(csr.vertexCount());
            header.putLong(csr.edgeCount());
            header.flip();
            emit(header, output, checksum);
            writeInts(offsets, output, checksum);
            writeInts(destinations, output, checksum);
            ByteBuffer trailer = allocate(8);
            trailer.putLong(checksum.getValue());
            trailer.flip();
            output.write(trailer);
        }
    }

    /**
     * Reads the snapshot at {@code source}.
     *
     * @throws SnapshotFormatException when the file is not a snapshot of this format, or its
     *     contents do not match the checksum it carries
     */
    public static CsrGraph read(InputFile source) throws IOException {
        Objects.requireNonNull(source, "Snapshot source cannot be null.");
        CRC32 checksum = new CRC32();
        try (SeekableInput input = source.open()) {
            ByteBuffer header = readExactly(input, HEADER_BYTES, checksum);
            long magic = header.getLong();
            if (magic != MAGIC) {
                throw new SnapshotFormatException(
                        "File at " + source.uri() + " is not a GraphAr CSR snapshot.");
            }
            int version = header.getInt();
            if (version != FORMAT_VERSION) {
                throw new SnapshotFormatException(
                        "Snapshot at "
                                + source.uri()
                                + " is format version "
                                + version
                                + ", but this reader writes and reads version "
                                + FORMAT_VERSION
                                + ".");
            }
            long vertexCount = header.getLong();
            long entryCount = header.getLong();
            ProjectionCapacity.requireAddressable(vertexCount, entryCount);
            long declared = snapshotBytes(vertexCount, entryCount);
            long actual = source.size();
            if (actual != declared) {
                throw new SnapshotFormatException(
                        "Snapshot at "
                                + source.uri()
                                + " declares "
                                + vertexCount
                                + " vertices and "
                                + entryCount
                                + " entries, which needs "
                                + declared
                                + " bytes, but the file is "
                                + actual
                                + " bytes.");
            }

            int[] offsets = readInts(input, Math.toIntExact(vertexCount + 1L), checksum);
            int[] destinations = readInts(input, Math.toIntExact(entryCount), checksum);
            long computed = checksum.getValue();
            long stored = readExactly(input, 8, null).getLong();
            if (computed != stored) {
                throw new SnapshotFormatException(
                        "Snapshot at "
                                + source.uri()
                                + " failed its checksum: the file carries "
                                + stored
                                + " but its contents hash to "
                                + computed
                                + ".");
            }
            requireConsistent(source, offsets, destinations, vertexCount, entryCount);
            return new CsrGraph(offsets, destinations);
        }
    }

    private static void requireConsistent(
            InputFile source, int[] offsets, int[] destinations, long vertexCount, long entryCount)
            throws SnapshotFormatException {
        if (offsets[0] != 0 || offsets[offsets.length - 1] != entryCount) {
            throw new SnapshotFormatException(
                    "Snapshot at "
                            + source.uri()
                            + " has offsets that do not span its "
                            + entryCount
                            + " entries.");
        }
        for (int vertex = 1; vertex < offsets.length; vertex++) {
            if (offsets[vertex] < offsets[vertex - 1]) {
                throw new SnapshotFormatException(
                        "Snapshot at "
                                + source.uri()
                                + " has a decreasing offset at vertex "
                                + vertex
                                + ".");
            }
        }
        for (int entry = 0; entry < destinations.length; entry++) {
            if (destinations[entry] < 0 || destinations[entry] >= vertexCount) {
                throw new SnapshotFormatException(
                        "Snapshot at "
                                + source.uri()
                                + " names vertex "
                                + destinations[entry]
                                + " at entry "
                                + entry
                                + ", which is outside its "
                                + vertexCount
                                + " vertices.");
            }
        }
    }

    private static void writeInts(int[] values, PositionOutput output, CRC32 checksum)
            throws IOException {
        ByteBuffer buffer = allocate(TRANSFER_BYTES);
        IntBuffer ints = buffer.asIntBuffer();
        int written = 0;
        while (written < values.length) {
            int batch = Math.min(ints.capacity(), values.length - written);
            ints.clear();
            ints.put(values, written, batch);
            buffer.position(0);
            buffer.limit(batch * Integer.BYTES);
            emit(buffer, output, checksum);
            buffer.clear();
            written += batch;
        }
    }

    private static int[] readInts(SeekableInput input, int count, CRC32 checksum)
            throws IOException {
        int[] values = new int[count];
        ByteBuffer buffer = allocate(TRANSFER_BYTES);
        int read = 0;
        while (read < count) {
            int batch = Math.min(buffer.capacity() / Integer.BYTES, count - read);
            buffer.clear();
            buffer.limit(batch * Integer.BYTES);
            input.readFully(buffer);
            buffer.flip();
            checksum.update(buffer);
            buffer.rewind();
            buffer.asIntBuffer().get(values, read, batch);
            read += batch;
        }
        return values;
    }

    private static ByteBuffer readExactly(SeekableInput input, int byteCount, CRC32 checksum)
            throws IOException {
        ByteBuffer buffer = allocate(byteCount);
        try {
            input.readFully(buffer);
        } catch (EOFException truncated) {
            throw new SnapshotFormatException(
                    "Snapshot ended after " + input.position() + " bytes.");
        }
        buffer.flip();
        if (checksum != null) {
            checksum.update(buffer);
            buffer.rewind();
        }
        return buffer;
    }

    /** Writes a buffer the caller has already positioned for reading. */
    private static void emit(ByteBuffer buffer, PositionOutput output, CRC32 checksum)
            throws IOException {
        checksum.update(buffer);
        buffer.rewind();
        output.write(buffer);
    }

    private static ByteBuffer allocate(int byteCount) {
        return ByteBuffer.allocate(byteCount).order(ByteOrder.LITTLE_ENDIAN);
    }
}
