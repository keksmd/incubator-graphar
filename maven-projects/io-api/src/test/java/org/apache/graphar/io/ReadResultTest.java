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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import org.junit.Test;

public class ReadResultTest {
    private final Schema schema =
            new Schema(List.of(new Field("id", ColumnType.of(ColumnType.Kind.INT64), false)));
    private final ReadRequest request =
            ReadRequest.builder(URI.create("file:///vertex/chunk0"))
                    .projection(Projection.of(List.of("id")))
                    .limit(10)
                    .build();

    @Test
    public void carriesTheCursorAndReportItWasBuiltWith() {
        BatchCursor cursor = new ListBatchCursor(schema, List.of());
        ReadReport report =
                new ReadReport(
                        EnumSet.of(ReadCapability.PROJECTION), EnumSet.of(ReadCapability.LIMIT));

        ReadResult result = new ReadResult(request, cursor, report);

        assertSame(cursor, result.cursor());
        assertSame(report, result.report());
    }

    @Test
    public void refusesAReportThatLeavesARequestedHintUnaccounted() {
        BatchCursor cursor = new ListBatchCursor(schema, List.of());
        ReadReport partial =
                new ReadReport(
                        EnumSet.of(ReadCapability.PROJECTION),
                        EnumSet.noneOf(ReadCapability.class));

        assertThrows(
                IllegalArgumentException.class, () -> new ReadResult(request, cursor, partial));
    }

    @Test
    public void refusesAReportThatClaimsAHintNobodyRequested() {
        BatchCursor cursor = new ListBatchCursor(schema, List.of());
        ReadReport extra =
                new ReadReport(
                        EnumSet.of(ReadCapability.PROJECTION, ReadCapability.FILTER),
                        EnumSet.of(ReadCapability.LIMIT));

        assertThrows(IllegalArgumentException.class, () -> new ReadResult(request, cursor, extra));
    }

    @Test
    public void refusesMissingParts() {
        BatchCursor cursor = new ListBatchCursor(schema, List.of());
        ReadReport report =
                new ReadReport(
                        EnumSet.of(ReadCapability.PROJECTION), EnumSet.of(ReadCapability.LIMIT));

        assertThrows(NullPointerException.class, () -> new ReadResult(null, cursor, report));
        assertThrows(NullPointerException.class, () -> new ReadResult(request, null, report));
        assertThrows(NullPointerException.class, () -> new ReadResult(request, cursor, null));
    }
}
