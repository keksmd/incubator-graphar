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
import static org.junit.Assert.assertThrows;

import java.time.Instant;
import org.junit.Test;

public class FilterTest {
    @Test
    public void retainsAnImmutableScalarComparison() {
        Filter filter = Filter.comparison("name", ComparisonOperator.EQUAL, Literal.of("alice"));

        assertEquals("name", filter.column());
        assertEquals(ComparisonOperator.EQUAL, filter.operator());
        assertEquals(Literal.of("alice"), filter.value());
        assertEquals(Filter.isNull("name"), Filter.isNull("name"));
    }

    @Test
    public void rejectsAmbiguousOrLossyComparisonValues() {
        assertThrows(IllegalArgumentException.class, () -> Literal.of(Double.NaN));
        assertThrows(
                IllegalArgumentException.class,
                () -> Literal.of(Instant.parse("2025-01-01T00:00:00.000000001Z")));
        assertThrows(
                IllegalArgumentException.class,
                () -> Filter.comparison("name", ComparisonOperator.IS_NULL, Literal.of("alice")));
    }
}
