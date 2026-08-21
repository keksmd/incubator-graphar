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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.graphar.core.ChunkRange;
import org.junit.Test;

/**
 * Proves an estimate answers what a rewrite would cost and reclaim, in the bytes the layout
 * reports, without any compaction taking place.
 */
public class CompactionEstimatorTest {
    @Test
    public void aChunkIsPricedAtItsOwnStoredBytesAndItsOwnPerEntryCost() {
        CompactionEstimator estimator = estimator(layout(), delta());

        CompactionEstimate estimate = estimator.estimate(0);

        assertEquals(0, estimate.chunkIndex());
        assertEquals(1, estimate.affectedChunks());
        assertEquals(1, estimate.affectedVertices());
        assertEquals(1_000, estimate.baseBytesTouched());
        assertEquals(100, estimate.baseEdgesTouched());
        assertEquals(10, estimate.deltaEntriesRemovedIfCompacted());
        assertEquals(500, estimate.deltaBytesRemovedIfCompacted());
        assertEquals(1_100, estimate.estimatedRewriteBytes());
        assertEquals(2.0d, estimate.compactionAmplification(), 1.0e-9d);
        assertEquals(500 / 1_100.0d, estimate.compactionUtility(), 1.0e-9d);
    }

    @Test
    public void aRewrittenChunkWithNoPatchesIsCostWithoutReturn() {
        CompactionEstimator estimator = estimator(layout(), delta());

        CompactionEstimate estimate = estimator.estimate(2);

        assertEquals(0, estimate.affectedChunks());
        assertEquals(0, estimate.affectedVertices());
        assertEquals(400, estimate.baseBytesTouched());
        assertEquals(400, estimate.estimatedRewriteBytes());
        assertEquals(Double.POSITIVE_INFINITY, estimate.compactionAmplification(), 0.0d);
        assertEquals(0.0d, estimate.compactionUtility(), 0.0d);
    }

    @Test
    public void aSetOfChunksCostsWhatItsMembersCostTogether() {
        CompactionEstimator estimator = estimator(layout(), delta());

        CompactionEstimate estimate = estimator.estimate(new long[] {0, 1, 0});

        assertArrayEquals(new long[] {0, 1}, estimate.chunkIndexes());
        assertEquals(2, estimate.rewrittenChunks());
        assertEquals(2, estimate.affectedChunks());
        assertEquals(3, estimate.affectedVertices());
        assertEquals(1_400, estimate.baseBytesTouched());
        assertEquals(700, estimate.deltaBytesRemovedIfCompacted());
        assertEquals(1_700, estimate.estimatedRewriteBytes());
    }

    @Test
    public void arangeIncludesTheChunksInsideItThatCarryNothing() {
        CompactionEstimator estimator = estimator(layout(), delta());

        CompactionEstimate estimate = estimator.estimate(new ChunkRange(0, 3));

        assertEquals(3, estimate.rewrittenChunks());
        assertEquals(2, estimate.affectedChunks());
        assertEquals(1_800, estimate.baseBytesTouched());
        assertEquals(2_100, estimate.estimatedRewriteBytes());
    }

    @Test
    public void foldingTheWholeDeltaRewritesExactlyTheChunksItPatches() {
        CompactionEstimator estimator = estimator(layout(), delta());

        CompactionEstimate estimate = estimator.estimateAll();

        assertArrayEquals(new long[] {0, 1}, estimate.chunkIndexes());
        assertEquals(700, estimate.deltaBytesRemovedIfCompacted());
        assertEquals(1_700, estimate.estimatedRewriteBytes());
    }

    @Test
    public void chunksAreRankedByWhatTheRewriteGivesBackPerByteWritten() {
        CompactionEstimator estimator = estimator(layout(), delta());

        List<CompactionEstimate> ranked = estimator.perChunk();

        assertEquals(2, ranked.size());
        assertEquals(0, ranked.get(0).chunkIndex());
        assertEquals(1, ranked.get(1).chunkIndex());
        assertTrue(ranked.get(0).compactionUtility() > ranked.get(1).compactionUtility());
    }

    @Test
    public void aPatchPastTheEndOfBaseCostsOnlyTheEntriesItAdds() {
        DeltaCostLedger ledger = new DeltaCostLedger(4);
        ledger.patched(41, 10, 400);

        CompactionEstimate estimate = estimator(layout(), ledger).estimate(10);

        assertEquals(0, estimate.baseBytesTouched());
        assertEquals(100, estimate.estimatedRewriteBytes());
        assertEquals(0.0d, estimate.compactionAmplification(), 0.0d);
        assertEquals(4.0d, estimate.compactionUtility(), 1.0e-9d);
    }

    @Test
    public void theHotSetIsTheChunksWhoseRewriteBuysTheLeast() {
        MeasuredChunkLayout.Builder layout = MeasuredChunkLayout.builder(4, 40);
        for (long chunkIndex = 0; chunkIndex < 9; chunkIndex++) {
            layout.chunk(chunkIndex, 100, 10);
        }
        layout.chunk(9, 100_000, 10_000);
        DeltaCostLedger ledger = new DeltaCostLedger(4);
        for (long chunkIndex = 0; chunkIndex < 10; chunkIndex++) {
            ledger.patched(chunkIndex * 4, 10, 1_000);
        }
        CompactionEstimator estimator = estimator(layout.build(), ledger);

        List<CompactionEstimate> retained = estimator.retainedInDelta(0.8d);
        List<CompactionEstimate> candidates = estimator.compactionCandidates(0.8d);

        assertEquals(1, retained.size());
        assertEquals(9, retained.get(0).chunkIndex());
        assertEquals(9, candidates.size());
        assertTrue(candidates.get(0).compactionUtility() > retained.get(0).compactionUtility());
        assertEquals(0.982d, estimator.rewriteConcentration().valueShareOfTopUnits(0.1d), 1.0e-3d);
    }

    @Test
    public void retainingNothingLeavesEveryPatchedChunkAsACandidate() {
        CompactionEstimator estimator = estimator(layout(), delta());

        assertEquals(0, estimator.retainedInDelta(0.0d).size());
        assertEquals(2, estimator.compactionCandidates(0.0d).size());
        assertEquals(2, estimator.retainedInDelta(1.0d).size());
        assertEquals(0, estimator.compactionCandidates(1.0d).size());
    }

    @Test
    public void readPenaltyOverAUniformWorkloadIsTheDeltaSpreadOverEveryVertex() {
        CompactionEstimator estimator = estimator(layout(), delta());

        ReadPenalty penalty = estimator.readPenalty(ReadProfile.uniformOverVertices(layout()));

        assertEquals(3.0d / 12.0d, penalty.probabilityOfPatchedRead(), 1.0e-9d);
        assertEquals(30.0d / 12.0d, penalty.expectedExtraEntriesPerRead(), 1.0e-9d);
        assertEquals(700.0d / 12.0d, penalty.expectedExtraBytesPerRead(), 1.0e-9d);
        assertEquals(180.0d / 12.0d, penalty.expectedBaseEntriesPerRead(), 1.0e-9d);
        assertEquals(210.0d / 180.0d, penalty.readAmplification(), 1.0e-9d);
    }

    @Test
    public void readPenaltyFollowsWhereTheReadsActuallyLand() {
        Map<Long, Long> reads = new HashMap<>();
        reads.put(1L, 100L);
        CompactionEstimator estimator = estimator(layout(), delta());

        ReadPenalty penalty = estimator.readPenalty(ReadProfile.ofCounts(reads));

        assertEquals(0.5d, penalty.probabilityOfPatchedRead(), 1.0e-9d);
        assertEquals(5.0d, penalty.expectedExtraEntriesPerRead(), 1.0e-9d);
        assertEquals(50.0d, penalty.expectedExtraBytesPerRead(), 1.0e-9d);
        assertEquals(10.0d, penalty.expectedBaseEntriesPerRead(), 1.0e-9d);
        assertEquals(1.5d, penalty.readAmplification(), 1.0e-9d);
    }

    @Test
    public void anEmptyDeltaCostsAReadNothing() {
        CompactionEstimator estimator = estimator(layout(), new DeltaCostLedger(4));

        ReadPenalty penalty = estimator.readPenalty(ReadProfile.uniformOverVertices(layout()));

        assertEquals(0.0d, penalty.expectedExtraEntriesPerRead(), 0.0d);
        assertEquals(1.0d, penalty.readAmplification(), 0.0d);
        assertEquals(0, estimator.estimateAll().rewrittenChunks());
    }

    @Test
    public void aLayoutAndADeltaCutIntoDifferentChunksAreRefused() {
        DeltaCostSnapshot mismatched = new DeltaCostLedger(8).snapshot();

        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new CompactionEstimator(layout(), mismatched));

        assertTrue(failure.getMessage().contains("vertex chunk size"));
    }

    private static CompactionEstimator estimator(BaseChunkLayout layout, DeltaCostLedger ledger) {
        return new CompactionEstimator(layout, ledger.snapshot());
    }

    private static MeasuredChunkLayout layout() {
        return MeasuredChunkLayout.builder(4, 12)
                .chunk(0, 1_000, 100)
                .chunk(1, 400, 40)
                .chunk(2, 400, 40)
                .build();
    }

    private static DeltaCostLedger delta() {
        DeltaCostLedger ledger = new DeltaCostLedger(4);
        ledger.patched(1, 10, 500);
        ledger.patched(5, 15, 100);
        ledger.patched(6, 5, 100);
        return ledger;
    }
}
