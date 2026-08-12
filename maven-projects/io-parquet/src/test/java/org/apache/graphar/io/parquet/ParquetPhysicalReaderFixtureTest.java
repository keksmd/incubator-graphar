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

package org.apache.graphar.io.parquet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ComparisonOperator;
import org.apache.graphar.io.Filter;
import org.apache.graphar.io.Literal;
import org.apache.graphar.io.Projection;
import org.apache.graphar.io.ReadCapability;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

public class ParquetPhysicalReaderFixtureTest {
    @Test
    public void readsProjectedFilteredRangeFromLdbcParquetFixture() throws IOException {
        Path fixture =
                Path.of(
                        "..",
                        "..",
                        "testing",
                        "ldbc_sample",
                        "parquet",
                        "vertex",
                        "person",
                        "firstName_lastName_gender",
                        "chunk0");
        ReadRequest request =
                ReadRequest.builder(fixture.toUri())
                        .projection(Projection.of(List.of("firstName")))
                        .rowRange(new RowRange(1, 12))
                        .filters(
                                List.of(
                                        Filter.comparison(
                                                "gender",
                                                ComparisonOperator.EQUAL,
                                                Literal.of("male"))))
                        .limit(2)
                        .build();

        ReadResult result = new ParquetPhysicalReader(new LocalStorage()).read(request);

        assertEquals(
                EnumSet.of(ReadCapability.PROJECTION, ReadCapability.LIMIT),
                result.report().applied());
        assertEquals(
                EnumSet.of(ReadCapability.ROW_RANGE, ReadCapability.FILTER),
                result.report().declined());
        assertEquals(List.of("Yacine", "Steve"), firstNames(result));
    }

    private static List<String> firstNames(ReadResult result) throws IOException {
        List<String> firstNames = new ArrayList<>();
        try (BatchCursor cursor = result.cursor()) {
            while (cursor.next()) {
                RecordBatch batch = cursor.batch();
                assertEquals(1, batch.schema().fields().size());
                assertEquals("firstName", batch.schema().fields().get(0).name());
                for (int index = 0; index < batch.rowCount(); index++) {
                    firstNames.add((String) batch.row(index).value(0));
                }
            }
            assertFalse(cursor.next());
        }
        return firstNames;
    }
}
