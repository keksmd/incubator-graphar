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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.net.URI;
import java.util.List;
import org.junit.Test;

public class WriteRequestTest {
    @Test
    public void bindsOutputIdentitySchemaAndDisposition() {
        Schema schema =
                new Schema(
                        List.of(
                                new Field("event_date", ColumnType.of(ColumnType.Kind.DATE), false),
                                new Field(
                                        "seen_at",
                                        ColumnType.of(ColumnType.Kind.TIMESTAMP_MILLIS),
                                        true)));
        WriteRequest request =
                new WriteRequest(URI.create("memory:/out"), schema, WriteMode.CREATE_NEW);

        assertEquals(URI.create("memory:/out"), request.uri());
        assertSame(schema, request.schema());
        assertEquals(WriteMode.CREATE_NEW, request.mode());
        assertEquals(WriteMode.APPEND, WriteMode.valueOf("APPEND"));
    }

    @Test
    public void rejectsMissingRequiredWriteContractValues() {
        Schema schema = new Schema(List.of());

        assertThrows(
                NullPointerException.class,
                () -> new WriteRequest(null, schema, WriteMode.OVERWRITE));
        assertThrows(
                NullPointerException.class,
                () -> new WriteRequest(URI.create("memory:/out"), null, WriteMode.OVERWRITE));
        assertThrows(
                NullPointerException.class,
                () -> new WriteRequest(URI.create("memory:/out"), schema, null));
    }

    @Test
    public void comparesEqualByUriSchemaAndDisposition() {
        Schema schema =
                new Schema(List.of(new Field("id", ColumnType.of(ColumnType.Kind.INT64), false)));
        WriteRequest first = new WriteRequest(URI.create("memory:/out"), schema, WriteMode.APPEND);
        WriteRequest same =
                new WriteRequest(
                        URI.create("memory:/out"),
                        new Schema(
                                List.of(
                                        new Field(
                                                "id",
                                                ColumnType.of(ColumnType.Kind.INT64),
                                                false))),
                        WriteMode.APPEND);
        WriteRequest differentMode =
                new WriteRequest(URI.create("memory:/out"), schema, WriteMode.OVERWRITE);

        assertEquals(first, same);
        assertEquals(first.hashCode(), same.hashCode());
        assertNotEquals(first, differentMode);
    }
}
