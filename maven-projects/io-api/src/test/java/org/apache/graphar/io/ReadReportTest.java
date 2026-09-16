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

package org.apache.graphar.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import java.util.EnumSet;
import java.util.Set;
import org.junit.Test;

public class ReadReportTest {
    @Test
    public void snapshotsAndSeparatesCapabilities() {
        Set<ReadCapability> applied = EnumSet.of(ReadCapability.PROJECTION);
        ReadReport report = new ReadReport(applied, EnumSet.of(ReadCapability.FILTER));
        applied.clear();

        assertEquals(EnumSet.of(ReadCapability.PROJECTION), report.applied());
        assertEquals(EnumSet.of(ReadCapability.FILTER), report.declined());
        assertThrows(
                UnsupportedOperationException.class,
                () -> report.applied().add(ReadCapability.LIMIT));
    }

    @Test
    public void comparesByAppliedAndDeclinedSets() {
        ReadReport report =
                new ReadReport(
                        EnumSet.of(ReadCapability.PROJECTION), EnumSet.of(ReadCapability.FILTER));
        ReadReport same =
                new ReadReport(
                        EnumSet.of(ReadCapability.PROJECTION), EnumSet.of(ReadCapability.FILTER));
        ReadReport swapped =
                new ReadReport(
                        EnumSet.of(ReadCapability.FILTER), EnumSet.of(ReadCapability.PROJECTION));

        assertEquals(report, same);
        assertEquals(report.hashCode(), same.hashCode());
        assertNotEquals(report, swapped);
        assertEquals(Set.of(report), Set.of(same));
    }

    @Test
    public void rejectsOverlappingOrNullCapabilitySets() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new ReadReport(
                                EnumSet.of(ReadCapability.PROJECTION),
                                EnumSet.of(ReadCapability.PROJECTION)));
        assertThrows(
                NullPointerException.class,
                () -> new ReadReport(null, EnumSet.noneOf(ReadCapability.class)));
    }
}
