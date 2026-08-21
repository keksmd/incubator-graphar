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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.graphar.reader.CsrDirection;
import org.apache.graphar.reader.HeterogeneousCsr;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Proves a delta that was made durable comes back as the delta that was written.
 *
 * <p>Recovery is what separates a cache from a store: the patches the delta holds are the freshest
 * state of the graph, and losing them on a restart would make every read after it wrong until the
 * next compaction. A crash can cut the log anywhere, so the cases here include the cut.
 */
public class FileDeltaJournalTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void aRecoveredDeltaAnswersWhatItAnsweredBeforeTheRestart() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long known = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        long other = DeltaFixture.unrelated(base, known);
        Path path = folder.getRoot().toPath().resolve("delta.journal");
        long arriving;
        long[] beforeRestart;
        long[] arrivingBefore;
        try (DeltaJournal journal = open(path, base)) {
            MutableDelta delta = MutableDelta.on(base, DeltaOptions.defaults(), journal);
            arriving = delta.vertexId(DeltaFixture.PERSON, "99000000000001");
            delta.append(known, other);
            delta.append(known, arriving);
            delta.sync();
            GraphView view = delta.seal();
            beforeRestart = view.neighbors(known);
            arrivingBefore = view.neighbors(arriving);
        }

        try (DeltaJournal reopened = open(path, base)) {
            MutableDelta recovered = MutableDelta.recover(base, DeltaOptions.defaults(), reopened);
            GraphView view = recovered.current();

            assertEquals(2L, view.deltaEdgeCount());
            assertEquals(arriving, view.globalIndex(DeltaFixture.PERSON, "99000000000001"));
            assertArrayEquals(beforeRestart, view.neighbors(known));
            assertArrayEquals(arrivingBefore, view.neighbors(arriving));
        }
    }

    @Test
    public void aTornTailIsTruncatedAndEverythingBeforeItSurvives() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long known = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        long other = DeltaFixture.unrelated(base, known);
        Path path = folder.getRoot().toPath().resolve("torn.journal");
        try (DeltaJournal journal = open(path, base)) {
            MutableDelta delta = MutableDelta.on(base, DeltaOptions.defaults(), journal);
            for (int edge = 0; edge < 8; edge++) {
                delta.append(known, other);
            }
            delta.sync();
        }
        long intact = path.toFile().length();
        try (FileChannel tearing =
                FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            tearing.write(ByteBuffer.wrap(new byte[] {2, 0, 0, 0}));
        }
        assertTrue(path.toFile().length() > intact);

        try (DeltaJournal reopened = open(path, base)) {
            MutableDelta recovered = MutableDelta.recover(base, DeltaOptions.defaults(), reopened);

            assertEquals(8L, recovered.current().deltaEdgeCount());
            assertEquals("the torn tail is gone from the file", intact, path.toFile().length());
        }
    }

    @Test
    public void aCorruptedRecordEndsTheReplayWhereItIsFound() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long known = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        long other = DeltaFixture.unrelated(base, known);
        Path path = folder.getRoot().toPath().resolve("corrupt.journal");
        long afterFirst;
        try (DeltaJournal journal = open(path, base)) {
            MutableDelta delta = MutableDelta.on(base, DeltaOptions.defaults(), journal);
            delta.append(known, other);
            delta.sync();
            afterFirst = path.toFile().length();
            delta.append(other, known);
            delta.append(known, other);
            delta.sync();
        }
        try (FileChannel damaging =
                FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            damaging.position(afterFirst + 1);
            damaging.write(ByteBuffer.wrap(new byte[] {(byte) 0xFF}));
        }

        try (DeltaJournal reopened = open(path, base)) {
            MutableDelta recovered = MutableDelta.recover(base, DeltaOptions.defaults(), reopened);

            assertEquals(1L, recovered.current().deltaEdgeCount());
        }
    }

    @Test
    public void aJournalOfAnotherBaseIsRefused() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        Path path = folder.getRoot().toPath().resolve("mismatched.journal");
        open(path, base).close();

        assertThrows(
                DeltaJournalFormatException.class,
                () -> FileDeltaJournal.open(path, base.vertexCount() + 1, CsrDirection.UNDIRECTED));
        assertThrows(
                DeltaJournalFormatException.class,
                () -> FileDeltaJournal.open(path, base.vertexCount(), CsrDirection.OUTGOING));
    }

    @Test
    public void aRewrittenJournalHoldsOnlyWhatTheRebasedDeltaHolds() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long known = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        long other = DeltaFixture.unrelated(base, known);
        Path path = folder.getRoot().toPath().resolve("rebased.journal");
        long[] afterRebase;
        try (DeltaJournal journal = open(path, base)) {
            MutableDelta delta = MutableDelta.on(base, DeltaOptions.defaults(), journal);
            delta.append(known, other);
            long absorbed = delta.sequence();
            delta.append(other, known);
            delta.sync();

            HeterogeneousCsr merged = base.merge(new long[] {known}, new long[] {other}, 1);
            GraphView rebased = delta.rebase(merged, absorbed);
            afterRebase = rebased.neighbors(known);

            assertEquals(1L, rebased.deltaEdgeCount());
        }

        HeterogeneousCsr merged = base.merge(new long[] {known}, new long[] {other}, 1);
        try (DeltaJournal reopened =
                FileDeltaJournal.open(path, merged.vertexCount(), CsrDirection.UNDIRECTED)) {
            MutableDelta recovered =
                    MutableDelta.recover(merged, DeltaOptions.defaults(), reopened);

            assertEquals(1L, recovered.current().deltaEdgeCount());
            assertArrayEquals(afterRebase, recovered.current().neighbors(known));
        }
    }

    private static DeltaJournal open(Path path, HeterogeneousCsr base) throws IOException {
        return FileDeltaJournal.open(path, base.vertexCount(), base.direction());
    }
}
