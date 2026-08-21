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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import org.junit.Test;

/** Proves the concentration a retention threshold is read from is measured, not assumed. */
public class ConcentrationProfileTest {
    @Test
    public void aDistributionWhereAFifthHoldsFourFifthsReadsAsExactlyThat() {
        long[] values = new long[100];
        Arrays.fill(values, 0, 20, 80L);
        Arrays.fill(values, 20, 100, 5L);

        ConcentrationProfile profile = ConcentrationProfile.of(values);

        assertEquals(2_000L, profile.total());
        assertEquals(0.8d, profile.valueShareOfTopUnits(0.2d), 1.0e-9d);
        assertEquals(20, profile.topUnitsForValueShare(0.8d));
        assertEquals(0.2d, profile.unitShareForValueShare(0.8d), 1.0e-9d);
    }

    @Test
    public void anEvenDistributionShowsNoConcentration() {
        long[] values = new long[10];
        Arrays.fill(values, 7L);

        ConcentrationProfile profile = ConcentrationProfile.of(values);

        assertEquals(0.0d, profile.gini(), 1.0e-9d);
        assertEquals(0.5d, profile.valueShareOfTopUnits(0.5d), 1.0e-9d);
        assertEquals(0.8d, profile.unitShareForValueShare(0.8d), 1.0e-9d);
    }

    @Test
    public void oneUnitHoldingEverythingIsAsConcentratedAsTheProfileGets() {
        long[] values = new long[10];
        values[3] = 1_000L;

        ConcentrationProfile profile = ConcentrationProfile.of(values);

        assertEquals(0.9d, profile.gini(), 1.0e-9d);
        assertEquals(1, profile.topUnitsForValueShare(1.0d));
        assertEquals(1.0d, profile.valueShareOfTopUnits(0.1d), 1.0e-9d);
    }

    @Test
    public void aShareSmallerThanOneUnitStillNamesOneUnit() {
        long[] values = {50L, 30L, 20L};

        ConcentrationProfile profile = ConcentrationProfile.of(values);

        assertEquals(0.5d, profile.valueShareOfTopUnits(0.01d), 1.0e-9d);
    }

    @Test
    public void theLorenzCurveRisesFromTheLightestUnitToTheWholeTotal() {
        long[] values = {1L, 2L, 7L};

        double[] curve = ConcentrationProfile.of(values).lorenzCurve();

        assertEquals(3, curve.length);
        assertEquals(0.1d, curve[0], 1.0e-9d);
        assertEquals(0.3d, curve[1], 1.0e-9d);
        assertEquals(1.0d, curve[2], 1.0e-9d);
        assertTrue(curve[0] <= curve[1] && curve[1] <= curve[2]);
    }

    @Test
    public void anEmptyOrZeroDistributionAnswersZeroRatherThanFailing() {
        ConcentrationProfile empty = ConcentrationProfile.of(new long[0]);
        ConcentrationProfile zeros = ConcentrationProfile.of(new long[] {0L, 0L});

        assertEquals(0, empty.unitCount());
        assertEquals(0.0d, empty.valueShareOfTopUnits(0.5d), 0.0d);
        assertEquals(0.0d, empty.gini(), 0.0d);
        assertEquals(0.0d, zeros.valueShareOfTopUnits(1.0d), 0.0d);
        assertEquals(0, zeros.topUnitsForValueShare(0.8d));
    }

    @Test
    public void negativeValuesAndSharesOutsideTheUnitIntervalAreRefused() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ConcentrationProfile.of(new long[] {1L, -1L}));
        ConcentrationProfile profile = ConcentrationProfile.of(new long[] {1L});
        assertThrows(IllegalArgumentException.class, () -> profile.valueShareOfTopUnits(1.5d));
        assertThrows(IllegalArgumentException.class, () -> profile.valueShareOfTopUnits(-0.1d));
    }
}
