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

package org.apache.graphar.compaction.cost;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.Test;

/** Proves the ledger reports the Delta a compaction decision would be taken over. */
public class DeltaCostLedgerTest {
    private static final Instant ORIGIN = Instant.parse("2026-08-21T10:00:00Z");

    @Test
    public void patchesAreCountedAgainstTheChunkOfTheVertexTheyChange() {
        DeltaCostLedger ledger = new DeltaCostLedger(4, new MovableClock(ORIGIN));

        ledger.patched(1, 3, 300);
        ledger.patched(2, 2, 200);
        ledger.patched(9, 5, 500);

        DeltaCostSnapshot snapshot = ledger.snapshot();
        assertEquals(3, snapshot.affectedVertices());
        assertEquals(2, snapshot.affectedChunks());
        assertEquals(10, snapshot.deltaEntries());
        assertEquals(1000, snapshot.deltaBytes());
        assertArrayEquals(new long[] {0, 2}, snapshot.chunkIndexes());
        assertEquals(2, snapshot.chunk(0).patchedVertices());
        assertEquals(5, snapshot.chunk(0).entries());
        assertEquals(500, snapshot.chunk(0).bytes());
        assertEquals(1, snapshot.chunk(2).patchedVertices());
        assertTrue(snapshot.chunk(1).isEmpty());
    }

    @Test
    public void repeatedPatchesToOneVertexAccumulateIntoOneEntry() {
        DeltaCostLedger ledger = new DeltaCostLedger(4, new MovableClock(ORIGIN));

        ledger.patched(7, 1, 10);
        ledger.patched(7, 2, 20);

        DeltaCostSnapshot snapshot = ledger.snapshot();
        assertEquals(1, snapshot.affectedVertices());
        assertEquals(3, snapshot.vertex(7).entries());
        assertEquals(30, snapshot.vertex(7).bytes());
    }

    @Test
    public void reclaimingAChunkRemovesItsPatchesAndNothingElse() {
        DeltaCostLedger ledger = new DeltaCostLedger(4, new MovableClock(ORIGIN));
        ledger.patched(1, 3, 300);
        ledger.patched(2, 2, 200);
        ledger.patched(9, 5, 500);

        ChunkDelta reclaimed = ledger.reclaim(0);

        assertEquals(0, reclaimed.chunkIndex());
        assertEquals(2, reclaimed.patchedVertices());
        assertEquals(5, reclaimed.entries());
        assertEquals(500, reclaimed.bytes());
        DeltaCostSnapshot snapshot = ledger.snapshot();
        assertEquals(1, snapshot.affectedVertices());
        assertEquals(500, snapshot.deltaBytes());
        assertEquals(500, snapshot.reclaimedBytes());
        assertEquals(1000, snapshot.admittedBytes());
        assertTrue(snapshot.chunk(0).isEmpty());
    }

    @Test
    public void reclaimingAChunkTheDeltaNeverTouchedGivesBackNothing() {
        DeltaCostLedger ledger = new DeltaCostLedger(4, new MovableClock(ORIGIN));
        ledger.patched(1, 3, 300);

        ChunkDelta reclaimed = ledger.reclaim(5);

        assertTrue(reclaimed.isEmpty());
        assertEquals(300, ledger.snapshot().deltaBytes());
        assertEquals(0, ledger.snapshot().reclaimedBytes());
    }

    @Test
    public void forgettingOneVertexReturnsWhatItHeld() {
        DeltaCostLedger ledger = new DeltaCostLedger(4, new MovableClock(ORIGIN));
        ledger.patched(1, 3, 300);

        VertexDelta forgotten = ledger.forget(1);
        VertexDelta unknown = ledger.forget(2);

        assertEquals(3, forgotten.entries());
        assertEquals(300, forgotten.bytes());
        assertEquals(0, unknown.entries());
        assertEquals(0, ledger.snapshot().affectedVertices());
        assertEquals(300, ledger.snapshot().reclaimedBytes());
    }

    @Test
    public void theGrowthRateSpansTheFirstPatchToTheSnapshotAndNotTheBusyPartOfIt() {
        MovableClock clock = new MovableClock(ORIGIN);
        DeltaCostLedger ledger = new DeltaCostLedger(4, clock);

        ledger.patched(1, 10, 1_000);
        clock.advance(Duration.ofSeconds(1));
        ledger.patched(2, 10, 1_000);
        clock.advance(Duration.ofSeconds(9));

        DeltaCostSnapshot snapshot = ledger.snapshot();
        assertEquals(Duration.ofSeconds(10), snapshot.observedWindow());
        assertEquals(200.0d, snapshot.growthBytesPerSecond(), 1.0e-9d);
        assertEquals(2.0d, snapshot.growthEntriesPerSecond(), 1.0e-9d);
        assertEquals(ORIGIN.plusSeconds(1), snapshot.lastArrival());
    }

    @Test
    public void compactionOutrunningIngestShowsAsNegativeNetGrowth() {
        MovableClock clock = new MovableClock(ORIGIN);
        DeltaCostLedger ledger = new DeltaCostLedger(4, clock);

        ledger.patched(1, 10, 1_000);
        clock.advance(Duration.ofSeconds(10));
        ledger.reclaim(0);

        DeltaCostSnapshot snapshot = ledger.snapshot();
        assertEquals(100.0d, snapshot.growthBytesPerSecond(), 1.0e-9d);
        assertEquals(0.0d, snapshot.netGrowthBytesPerSecond(), 1.0e-9d);
        assertEquals(0, snapshot.deltaBytes());
    }

    @Test
    public void anEmptyLedgerReportsNoGrowthRatherThanAnUndefinedOne() {
        DeltaCostSnapshot snapshot = new DeltaCostLedger(4, new MovableClock(ORIGIN)).snapshot();

        assertEquals(0, snapshot.affectedVertices());
        assertEquals(Duration.ZERO, snapshot.observedWindow());
        assertEquals(0.0d, snapshot.growthBytesPerSecond(), 0.0d);
        assertEquals(0, snapshot.chunkIndexes().length);
    }

    @Test
    public void aSnapshotIsUnaffectedByLaterPatches() {
        DeltaCostLedger ledger = new DeltaCostLedger(4, new MovableClock(ORIGIN));
        ledger.patched(1, 3, 300);

        DeltaCostSnapshot taken = ledger.snapshot();
        ledger.patched(2, 4, 400);

        assertEquals(300, taken.deltaBytes());
        assertEquals(1, taken.affectedVertices());
        assertEquals(700, ledger.snapshot().deltaBytes());
    }

    @Test
    public void theHeaviestPatchedVerticesComeFirst() {
        DeltaCostLedger ledger = new DeltaCostLedger(4, new MovableClock(ORIGIN));
        ledger.patched(1, 1, 100);
        ledger.patched(2, 1, 900);
        ledger.patched(3, 1, 500);

        DeltaCostSnapshot snapshot = ledger.snapshot();

        assertEquals(2, snapshot.vertices().get(0).vertex());
        assertEquals(3, snapshot.vertices().get(1).vertex());
        assertEquals(1, snapshot.vertices().get(2).vertex());
    }

    @Test
    public void negativeQuantitiesAreRefusedRatherThanRecorded() {
        DeltaCostLedger ledger = new DeltaCostLedger(4, new MovableClock(ORIGIN));

        assertThrows(IllegalArgumentException.class, () -> ledger.patched(-1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> ledger.patched(1, -1, 1));
        assertThrows(IllegalArgumentException.class, () -> ledger.patched(1, 1, -1));
        assertThrows(IllegalArgumentException.class, () -> ledger.reclaim(-1));
        assertThrows(IllegalArgumentException.class, () -> new DeltaCostLedger(0));
    }

    private static final class MovableClock extends Clock {
        private Instant now;

        private MovableClock(Instant now) {
            this.now = now;
        }

        private void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
