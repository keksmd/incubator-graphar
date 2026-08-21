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

import java.util.Arrays;

/**
 * How unevenly a non-negative quantity is spread over the units carrying it.
 *
 * <p>The retention policy is stated as a proportion - a small share of vertices or chunks accounts
 * for most of the work - and a proportion asserted from memory is a guess. This class turns the
 * assertion into a reading of the distribution actually observed: {@link
 * #valueShareOfTopUnits(double)} answers what the heaviest fifth really accounts for, {@link
 * #unitShareForValueShare(double)} answers how small the set behind four fifths of the work really
 * is, and {@link #gini()} summarizes the whole curve. Whether the answer is near eighty-twenty is
 * an outcome here, never an input.
 */
public final class ConcentrationProfile {
    /**
     * Absorbs the floating-point error in a share, so that asking for a fifth of a hundred units
     * answers twenty units rather than twenty-one.
     */
    private static final double ROUNDING_SLACK = 1.0e-9d;

    private final long[] descending;
    private final long[] prefixSums;
    private final long total;

    private ConcentrationProfile(long[] descending, long[] prefixSums, long total) {
        this.descending = descending;
        this.prefixSums = prefixSums;
        this.total = total;
    }

    /** Builds a profile over one non-negative value per unit. */
    public static ConcentrationProfile of(long[] values) {
        long[] sorted = values.clone();
        for (long value : sorted) {
            if (value < 0) {
                throw new IllegalArgumentException("Values must be non-negative: " + value);
            }
        }
        Arrays.sort(sorted);
        for (int index = 0; index < sorted.length / 2; index++) {
            long swap = sorted[index];
            sorted[index] = sorted[sorted.length - 1 - index];
            sorted[sorted.length - 1 - index] = swap;
        }
        long[] prefixSums = new long[sorted.length + 1];
        for (int index = 0; index < sorted.length; index++) {
            prefixSums[index + 1] = prefixSums[index] + sorted[index];
        }
        return new ConcentrationProfile(sorted, prefixSums, prefixSums[sorted.length]);
    }

    /** Returns how many units the profile covers. */
    public int unitCount() {
        return descending.length;
    }

    /** Returns the sum over all units. */
    public long total() {
        return total;
    }

    /**
     * Returns the share of the total held by the heaviest {@code unitShare} of units, rounding the
     * unit count up so a share smaller than one unit still names one.
     */
    public double valueShareOfTopUnits(double unitShare) {
        checkShare(unitShare);
        if (descending.length == 0 || total == 0) {
            return 0.0d;
        }
        double exactUnits = unitShare * descending.length;
        int units = (int) Math.min(descending.length, Math.ceil(exactUnits - ROUNDING_SLACK));
        return prefixSums[units] / (double) total;
    }

    /**
     * Returns how many of the heaviest units are needed to reach {@code valueShare} of the total.
     */
    public int topUnitsForValueShare(double valueShare) {
        checkShare(valueShare);
        if (descending.length == 0 || total == 0) {
            return 0;
        }
        double target = valueShare * total - ROUNDING_SLACK * total;
        for (int units = 1; units <= descending.length; units++) {
            if (prefixSums[units] >= target) {
                return units;
            }
        }
        return descending.length;
    }

    /** Returns the share of units needed to reach {@code valueShare} of the total. */
    public double unitShareForValueShare(double valueShare) {
        if (descending.length == 0) {
            return 0.0d;
        }
        return topUnitsForValueShare(valueShare) / (double) descending.length;
    }

    /**
     * Returns the Gini coefficient of the distribution: zero when every unit carries the same
     * amount, approaching one as a single unit carries everything.
     */
    public double gini() {
        int count = descending.length;
        if (count == 0 || total == 0) {
            return 0.0d;
        }
        double weighted = 0.0d;
        for (int index = 0; index < count; index++) {
            weighted += (index + 1L) * (double) descending[count - 1 - index];
        }
        return (2.0d * weighted) / (count * (double) total) - (count + 1.0d) / count;
    }

    /**
     * Returns the Lorenz curve as cumulative value shares over units taken lightest first, one
     * entry per unit. Plotted against equal unit steps this is the curve the policy threshold is
     * read off.
     */
    public double[] lorenzCurve() {
        double[] curve = new double[descending.length];
        if (descending.length == 0) {
            return curve;
        }
        long cumulative = 0;
        for (int index = 0; index < descending.length; index++) {
            cumulative += descending[descending.length - 1 - index];
            curve[index] = total == 0 ? 0.0d : cumulative / (double) total;
        }
        return curve;
    }

    private static void checkShare(double share) {
        if (!(share >= 0.0d) || share > 1.0d) {
            throw new IllegalArgumentException("Share must be within [0, 1]: " + share);
        }
    }
}
