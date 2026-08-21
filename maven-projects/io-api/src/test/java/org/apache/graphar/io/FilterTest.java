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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.Test;

public class FilterTest {
    private static final ColumnRef NAME = ColumnRef.of("name");
    private static final ColumnRef AGE = ColumnRef.of("age");
    private static final Filter ADULT = Filter.greaterThanOrEqual(AGE, Literal.of(18));
    private static final Filter NAMED = Filter.isNotNull(NAME);
    private static final Filter ALICE = Filter.equal(NAME, Literal.of("alice"));

    @Test
    public void retainsAnImmutableScalarComparison() {
        Filter.Comparison filter = (Filter.Comparison) ALICE;

        assertEquals(NAME, filter.column());
        assertEquals(ComparisonOperator.EQUAL, filter.operator());
        assertEquals(Literal.of("alice"), filter.value().get());
        assertEquals(filter, Filter.equal(ColumnRef.of("name"), Literal.of("alice")));
        assertEquals(filter.hashCode(), Filter.equal(NAME, Literal.of("alice")).hashCode());
    }

    @Test
    public void namesEveryComparisonOperator() {
        assertEquals(ComparisonOperator.NOT_EQUAL, operator(Filter.notEqual(NAME, Literal.of(1))));
        assertEquals(ComparisonOperator.LESS_THAN, operator(Filter.lessThan(NAME, Literal.of(1))));
        assertEquals(
                ComparisonOperator.LESS_THAN_OR_EQUAL,
                operator(Filter.lessThanOrEqual(NAME, Literal.of(1))));
        assertEquals(
                ComparisonOperator.GREATER_THAN, operator(Filter.greaterThan(NAME, Literal.of(1))));
        assertEquals(
                ComparisonOperator.GREATER_THAN_OR_EQUAL,
                operator(Filter.greaterThanOrEqual(NAME, Literal.of(1))));
    }

    @Test
    public void nullChecksCarryNoValue() {
        Filter.Comparison isNull = (Filter.Comparison) Filter.isNull(NAME);
        Filter.Comparison isNotNull = (Filter.Comparison) NAMED;

        assertEquals(ComparisonOperator.IS_NULL, isNull.operator());
        assertEquals(ComparisonOperator.IS_NOT_NULL, isNotNull.operator());
        assertFalse(isNull.value().isPresent());
        assertFalse(isNotNull.value().isPresent());
        assertEquals(Filter.isNull(NAME), isNull);
        assertNotEquals(isNull, isNotNull);
    }

    @Test
    public void composesWithAndOrAndNot() {
        Filter filter = NAMED.and(ADULT).or(ALICE.negate());

        Filter.Or or = (Filter.Or) filter;
        Filter.And and = (Filter.And) or.operands().get(0);
        Filter.Not not = (Filter.Not) or.operands().get(1);
        assertEquals(List.of(NAMED, ADULT), and.operands());
        assertEquals(ALICE, not.operand());
        assertEquals(filter, Filter.or(Filter.and(NAMED, ADULT), Filter.not(ALICE)));
        assertEquals(
                filter.hashCode(),
                Filter.or(Filter.and(NAMED, ADULT), Filter.not(ALICE)).hashCode());
        assertNotEquals(Filter.and(NAMED, ADULT), Filter.or(NAMED, ADULT));
        assertNotEquals(Filter.and(NAMED, ADULT), Filter.and(ADULT, NAMED));
    }

    @Test
    public void flattensSameKindJunctionsAndCollapsesTrivialOnes() {
        assertEquals(Filter.and(NAMED, ADULT, ALICE), NAMED.and(ADULT).and(ALICE));
        assertEquals(Filter.and(NAMED, ADULT, ALICE), NAMED.and(ADULT.and(ALICE)));
        assertEquals(Filter.or(NAMED, ADULT, ALICE), NAMED.or(ADULT).or(ALICE));
        assertEquals(
                List.of(NAMED, Filter.or(ADULT, ALICE)),
                ((Filter.And) NAMED.and(ADULT.or(ALICE))).operands());
        assertSame(NAMED, Filter.and(List.of(NAMED)));
        assertSame(NAMED, Filter.or(List.of(NAMED)));
        assertSame(NAMED, NAMED.negate().negate());
        assertThrows(
                UnsupportedOperationException.class,
                () -> ((Filter.And) NAMED.and(ADULT)).operands().add(ALICE));
    }

    @Test
    public void visitsEveryNodeKind() {
        Filter filter = NAMED.and(ADULT).or(ALICE.negate());

        String rendered =
                filter.accept(
                        new Filter.Visitor<String>() {
                            @Override
                            public String comparison(Filter.Comparison node) {
                                return node.column().name();
                            }

                            @Override
                            public String and(Filter.And node) {
                                return "(" + join(node, " & ") + ")";
                            }

                            @Override
                            public String or(Filter.Or node) {
                                return "(" + join(node, " | ") + ")";
                            }

                            @Override
                            public String not(Filter.Not node) {
                                return "!" + node.operand().accept(this);
                            }

                            private String join(Filter.Junction node, String glue) {
                                StringBuilder out = new StringBuilder();
                                for (Filter operand : node.operands()) {
                                    if (out.length() > 0) {
                                        out.append(glue);
                                    }
                                    out.append(operand.accept(this));
                                }
                                return out.toString();
                            }
                        });

        assertEquals("((name & age) | !name)", rendered);
        assertTrue(filter.toString().startsWith("OR["));
    }

    @Test
    public void rejectsAmbiguousOrLossyComparisonValues() {
        assertThrows(IllegalArgumentException.class, () -> Literal.of(Double.NaN));
        assertThrows(
                IllegalArgumentException.class,
                () -> Literal.of(Instant.parse("2025-01-01T00:00:00.000000001Z")));
        assertThrows(NullPointerException.class, () -> Filter.equal(NAME, null));
        assertThrows(NullPointerException.class, () -> Filter.isNull(null));
        assertThrows(IllegalArgumentException.class, () -> Filter.and(List.of()));
        assertThrows(IllegalArgumentException.class, () -> Filter.or(List.of()));
        assertThrows(NullPointerException.class, () -> Filter.and(NAMED, null));
        assertThrows(NullPointerException.class, () -> Filter.not(null));
    }

    private static ComparisonOperator operator(Filter filter) {
        return ((Filter.Comparison) filter).operator();
    }
}
