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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.junit.Test;

public class ReadRequestTest {
    @Test
    public void snapshotsAllReadHints() {
        List<ColumnRef> columns =
                new ArrayList<>(List.of(ColumnRef.of("dst"), ColumnRef.of("weight")));
        List<Filter> filters =
                new ArrayList<>(
                        List.of(
                                Filter.greaterThanOrEqual(
                                        ColumnRef.of("weight"), Literal.of(1.5D))));

        ReadRequest request =
                ReadRequest.builder(URI.create("file:/dataset/part0"))
                        .projection(Projection.of(columns))
                        .rowRange(new RowRange(4, 9))
                        .filters(filters)
                        .limit(0)
                        .build();

        columns.clear();
        filters.clear();

        assertEquals(
                List.of(ColumnRef.of("dst"), ColumnRef.of("weight")),
                request.projection().columns());
        assertEquals(1, request.filters().size());
        assertEquals(new RowRange(4, 9), request.rowRange().get());
        assertTrue(request.limit().isPresent());
        assertEquals(0L, request.limit().getAsLong());
        assertEquals(EnumSet.allOf(ReadCapability.class), request.requestedCapabilities());
        assertThrows(
                UnsupportedOperationException.class,
                () -> request.filters().add(Filter.isNotNull(ColumnRef.of("weight"))));
    }

    @Test
    public void defaultsDoNotRequestPushdown() {
        ReadRequest request = ReadRequest.builder(URI.create("memory:/input")).build();

        assertTrue(request.projection().isAllColumns());
        assertFalse(request.rowRange().isPresent());
        assertFalse(request.limit().isPresent());
        assertTrue(request.requestedCapabilities().isEmpty());
    }

    @Test
    public void rejectsInvalidHints() {
        assertThrows(IllegalArgumentException.class, () -> new RowRange(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> new RowRange(2, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> ReadRequest.builder(URI.create("file:/input")).limit(-1));
        assertThrows(
                IllegalArgumentException.class,
                () -> Projection.of(ColumnRef.of("id"), ColumnRef.of("id")));
        assertThrows(
                IllegalArgumentException.class, () -> Literal.of(new StringBuilder("mutable")));
    }

    @Test
    public void comparesByEveryHint() {
        ReadRequest request = request().build();

        assertEquals(request, request().build());
        assertEquals(request.hashCode(), request().build().hashCode());
        assertNotEquals(request, request().limit(2).build());
        assertNotEquals(request, request().rowRange(new RowRange(0, 1)).build());
        assertNotEquals(request, request().filters(List.of()).build());
        assertNotEquals(request, request().projection(Projection.all()).build());
        assertNotEquals(request, ReadRequest.builder(URI.create("file:/other")).build());
    }

    private static ReadRequest.Builder request() {
        return ReadRequest.builder(URI.create("file:/input"))
                .projection(Projection.of(ColumnRef.of("id")))
                .filters(List.of(Filter.isNotNull(ColumnRef.of("id"))))
                .limit(1);
    }
}
