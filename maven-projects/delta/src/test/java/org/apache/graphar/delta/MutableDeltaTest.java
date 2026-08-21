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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import org.apache.graphar.reader.HeterogeneousCsr;
import org.junit.Test;

/**
 * Proves the delta accepts adjacency at request rate and shows it only when it is published.
 *
 * <p>The point of the delta is freshness that a projection rebuild cannot give, so what has to hold
 * is that an append is cheap, that a reader never sees half of one, and that the delta stops
 * growing where its budget says it does.
 */
public class MutableDeltaTest {
    @Test
    public void anAppendedEdgeIsInvisibleUntilItIsSealed() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long first = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        long second = DeltaFixture.unrelated(base, first);
        MutableDelta delta = MutableDelta.on(base);
        GraphView before = delta.current();

        delta.append(first, second);

        assertEquals("an unsealed append is not published", 0L, before.deltaEdgeCount());
        assertEquals(
                "an unsealed append is not published", 0L, delta.current().delta().degree(first));

        GraphView after = delta.seal();

        assertEquals(1L, after.deltaEdgeCount());
        assertEquals(1, after.delta().degree(first));
        assertEquals(1, after.delta().degree(second));
    }

    @Test
    public void aPublishedViewKeepsAnsweringWhileTheDeltaGrows() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long first = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        long second = DeltaFixture.unrelated(base, first);
        MutableDelta delta = MutableDelta.on(base);
        delta.append(first, second);
        GraphView published = delta.seal();
        long[] served = published.neighbors(first);

        for (int edge = 0; edge < 64; edge++) {
            delta.append(first, second);
        }

        assertArrayEquals(
                "a published view must not observe what was appended after it",
                served,
                published.neighbors(first));
        assertEquals(1L, published.deltaEdgeCount());
        assertEquals(65L, delta.seal().deltaEdgeCount());
    }

    @Test
    public void theDeltaNamesVerticesItsBaseDoesNotHold() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        MutableDelta delta = MutableDelta.on(base);
        long known = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);

        long arriving = delta.vertexId(DeltaFixture.PERSON, "99000000000001");
        long repeated = delta.vertexId(DeltaFixture.PERSON, "99000000000001");
        long another = delta.vertexId(DeltaFixture.PERSON, "99000000000002");
        delta.append(known, arriving);
        GraphView view = delta.seal();

        assertEquals("a named vertex keeps its number", arriving, repeated);
        assertEquals(base.vertexCount(), arriving);
        assertEquals(base.vertexCount() + 1, another);
        assertEquals(base.vertexCount() + 2, view.vertexCount());
        assertTrue(view.isOverlay(arriving));
        assertFalse(view.isOverlay(known));
        assertEquals(DeltaFixture.PERSON, view.typeOf(arriving));
        assertEquals(arriving, view.globalIndex(DeltaFixture.PERSON, "99000000000001"));
        assertArrayEquals(new long[] {known}, view.neighbors(arriving));
    }

    @Test
    public void anIdentifierTheBaseHoldsIsNotNamedAgain() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        MutableDelta delta = MutableDelta.on(base);

        long resolved = delta.vertexId(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);

        assertEquals(base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID), resolved);
        assertEquals(0, delta.overlayCount());
    }

    @Test
    public void theDeltaRefusesMoreEdgesThanItsCeiling() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long first = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        long second = DeltaFixture.unrelated(base, first);
        MutableDelta delta =
                MutableDelta.on(
                        base,
                        DeltaOptions.defaults().maxEdges(4).chunkSize(2),
                        DeltaJournal.none());
        for (int edge = 0; edge < 4; edge++) {
            delta.append(first, second);
        }

        DeltaCapacityExceededException refused =
                assertThrows(
                        DeltaCapacityExceededException.class, () -> delta.append(first, second));

        assertEquals(4L, refused.edgeCount());
        assertEquals(4L, refused.maxEdges());
        assertEquals(
                "a refused append leaves the delta as it was", 4L, delta.seal().deltaEdgeCount());
    }

    @Test
    public void anAppendOutsideTheViewIsRefused() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        MutableDelta delta = MutableDelta.on(base);

        assertThrows(IllegalArgumentException.class, () -> delta.append(0L, base.vertexCount()));
        assertThrows(IllegalArgumentException.class, () -> delta.append(-1L, 0L));
    }

    @Test
    public void rebasingDropsExactlyTheEdgesTheNewBaseAbsorbed() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long first = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        long second = DeltaFixture.unrelated(base, first);
        MutableDelta delta = MutableDelta.on(base);
        delta.append(first, second);
        long absorbed = delta.sequence();
        delta.append(second, first);
        long[] beforeRebase = delta.seal().neighbors(first);

        HeterogeneousCsr merged = base.merge(new long[] {first}, new long[] {second}, 1);
        GraphView rebased = delta.rebase(merged, absorbed);

        assertEquals("the absorbed edge is gone from the delta", 1L, rebased.deltaEdgeCount());
        assertEquals(merged.edgeCount(), rebased.baseEdgeCount());
        assertArrayEquals(
                "a rebase must not change what a read answers",
                beforeRebase,
                rebased.neighbors(first));
    }

    @Test
    public void rebasingKeepsTheVertexNumbersTheDeltaHandedOut() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long known = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        MutableDelta delta = MutableDelta.on(base);
        long arriving = delta.vertexId(DeltaFixture.PERSON, "99000000000001");
        delta.append(known, arriving);
        long[] before = delta.seal().neighbors(arriving);

        GraphView rebased = delta.rebase(base, delta.baseSequence());

        assertEquals(arriving, rebased.globalIndex(DeltaFixture.PERSON, "99000000000001"));
        assertArrayEquals(before, rebased.neighbors(arriving));
        assertEquals(1L, rebased.deltaEdgeCount());
    }

    @Test
    public void rebasingRefusesABaseTheDeltaWasNotTakenAgainst() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        MutableDelta delta = MutableDelta.on(base);
        delta.append(0L, 1L);

        assertThrows(
                IllegalArgumentException.class, () -> delta.rebase(base, delta.sequence() + 1));
        assertThrows(
                IllegalArgumentException.class, () -> delta.rebase(base, delta.baseSequence() - 1));
    }

    @Test
    public void aPatchedVertexIsCountedOnceHoweverManyEdgesItTook() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long first = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        long second = DeltaFixture.unrelated(base, first);
        MutableDelta delta = MutableDelta.on(base);
        for (int edge = 0; edge < 32; edge++) {
            delta.append(first, second);
        }

        DeltaSnapshot snapshot = delta.seal().delta();

        assertEquals(2, snapshot.patchedVertexCount());
        assertEquals(32, snapshot.degree(first));
        assertEquals(32, snapshot.degree(second));
        long[] repeated = snapshot.neighbors(first);
        assertEquals(32, repeated.length);
        assertArrayEquals(
                "multiplicity is preserved", filled(32, second), Arrays.stream(repeated).toArray());
    }

    private static long[] filled(int count, long value) {
        long[] values = new long[count];
        Arrays.fill(values, value);
        return values;
    }
}
