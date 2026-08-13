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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Proves a projection survives a round trip through storage, and that a snapshot which cannot be
 * trusted is refused rather than served.
 */
public class CsrSnapshotTest {
    private static final long MAX_VERTICES = 10_000L;
    private static final long MAX_EDGES = 100_000L;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final LocalStorage storage = new LocalStorage();

    @Test
    public void aProjectionReadBackFromStorageIsTheProjectionThatWasWritten() throws Exception {
        for (CsrDirection direction : CsrDirection.values()) {
            CsrGraph built = fixtureCsr(direction);
            URI target = uriFor("fixture-" + direction + ".csr");

            CsrSnapshot.write(built, storage.outputFile(target));
            CsrGraph loaded = CsrSnapshot.read(storage.inputFile(target));

            assertEquals(direction.name(), built.vertexCount(), loaded.vertexCount());
            assertEquals(direction.name(), built.edgeCount(), loaded.edgeCount());
            assertArrayEquals(direction.name(), built.offsets(), loaded.offsets());
            assertArrayEquals(direction.name(), built.destinations(), loaded.destinations());
            assertEquals(
                    CsrSnapshot.snapshotBytes(built.vertexCount(), built.edgeCount()),
                    Files.size(Path.of(target)));
        }
    }

    @Test
    public void aSnapshotOfAGraphWithoutEdgesRoundTrips() throws Exception {
        CsrGraph empty =
                CsrMaterializer.fromEndpoints(
                        new int[0], new int[0], 0, 5L, CsrDirection.UNDIRECTED);
        URI target = uriFor("empty.csr");

        CsrSnapshot.write(empty, storage.outputFile(target));
        CsrGraph loaded = CsrSnapshot.read(storage.inputFile(target));

        assertEquals(5L, loaded.vertexCount());
        assertEquals(0L, loaded.edgeCount());
        assertArrayEquals(empty.offsets(), loaded.offsets());
    }

    @Test
    public void aTruncatedSnapshotIsRefusedInsteadOfServingAGraphMissingEdges() throws Exception {
        URI target = writeFixture("truncated.csr");
        Path file = Path.of(target);
        byte[] whole = Files.readAllBytes(file);
        Files.write(file, java.util.Arrays.copyOf(whole, whole.length - 64));

        SnapshotFormatException refused =
                assertThrows(
                        SnapshotFormatException.class,
                        () -> CsrSnapshot.read(storage.inputFile(target)));
        assertTrue(refused.getMessage(), refused.getMessage().contains("but the file is"));
    }

    @Test
    public void aSnapshotWithOneFlippedBitIsRefusedByItsChecksum() throws Exception {
        URI target = writeFixture("corrupt.csr");
        Path file = Path.of(target);
        byte[] whole = Files.readAllBytes(file);
        int entry = whole.length / 2;
        whole[entry] = (byte) (whole[entry] ^ 0x01);
        Files.write(file, whole);

        SnapshotFormatException refused =
                assertThrows(
                        SnapshotFormatException.class,
                        () -> CsrSnapshot.read(storage.inputFile(target)));
        assertTrue(refused.getMessage(), refused.getMessage().contains("checksum"));
    }

    @Test
    public void aFileThatIsNotASnapshotIsNamedAsSuch() throws Exception {
        URI target = uriFor("stranger.csr");
        Files.write(Path.of(target), new byte[128]);

        SnapshotFormatException refused =
                assertThrows(
                        SnapshotFormatException.class,
                        () -> CsrSnapshot.read(storage.inputFile(target)));
        assertTrue(refused.getMessage(), refused.getMessage().contains("not a GraphAr CSR"));
    }

    @Test
    public void aLargeProjectionRoundTripsThroughMoreThanOneTransferBuffer() throws Exception {
        int vertices = 400_000;
        int edges = 1_200_000;
        int[] sources = new int[edges];
        int[] targets = new int[edges];
        Random random = new Random(20260813L);
        for (int edge = 0; edge < edges; edge++) {
            sources[edge] = random.nextInt(vertices);
            targets[edge] = random.nextInt(vertices);
        }
        CsrGraph built =
                CsrMaterializer.fromEndpoints(
                        sources, targets, edges, vertices, CsrDirection.UNDIRECTED);
        URI target = uriFor("large.csr");

        CsrSnapshot.write(built, storage.outputFile(target));
        CsrGraph loaded = CsrSnapshot.read(storage.inputFile(target));

        assertArrayEquals(built.rawOffsets(), loaded.rawOffsets());
        assertArrayEquals(built.rawDestinations(), loaded.rawDestinations());
    }

    private URI writeFixture(String name) throws IOException {
        URI target = uriFor(name);
        CsrSnapshot.write(fixtureCsr(CsrDirection.UNDIRECTED), storage.outputFile(target));
        return target;
    }

    private URI uriFor(String name) {
        return temporaryFolder.getRoot().toPath().resolve(name).toUri();
    }

    private static CsrGraph fixtureCsr(CsrDirection direction) throws IOException {
        return GraphReader.open(
                        Path.of("..", "..", "testing", "ldbc_sample", "parquet")
                                .resolve("ldbc_sample.graph.yml")
                                .toUri(),
                        new LocalFileSystemStringGraphInfoLoader(),
                        new LocalStorage(),
                        new ParquetPhysicalReader(new LocalStorage()))
                .edge("person", "knows", "person")
                .materializeCsr(MAX_VERTICES, MAX_EDGES, direction);
    }
}
