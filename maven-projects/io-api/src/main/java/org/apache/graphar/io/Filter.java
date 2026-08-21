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

import java.util.Objects;

/**
 * A single inspectable table-filter hint; request filters are combined with logical AND. A physical
 * reader must validate a comparison literal against the column's {@link ColumnType}: BOOLEAN uses
 * Boolean; integer and floating kinds use their matching boxed Java types; STRING uses String; DATE
 * uses LocalDate; and TIMESTAMP_MILLIS uses millisecond-precise Instant. Other types cannot be
 * compared by this contract.
 *
 * <p>Comparison operands must have the same declared type; a type mismatch is invalid rather than a
 * coercion. Null values never match a comparison, including NOT_EQUAL; use IS_NULL or IS_NOT_NULL
 * for null tests. Ordered STRING comparisons use {@link String#compareTo(String)}, and DATE and
 * TIMESTAMP_MILLIS use their natural ordering. Readers must reject an invalid comparison before
 * returning a result, whether the filter is pushed down or evaluated as fallback.
 */
public final class Filter {
    private final String column;
    private final ComparisonOperator operator;
    private final Literal value;

    private Filter(String column, ComparisonOperator operator, Literal value) {
        if (column == null || column.isBlank()) {
            throw new IllegalArgumentException("A filter column cannot be blank.");
        }
        this.column = column;
        this.operator = Objects.requireNonNull(operator, "Filter operator cannot be null.");
        if ((operator == ComparisonOperator.IS_NULL || operator == ComparisonOperator.IS_NOT_NULL)
                && value != null) {
            throw new IllegalArgumentException(operator + " does not accept a comparison value.");
        }
        if (operator != ComparisonOperator.IS_NULL
                && operator != ComparisonOperator.IS_NOT_NULL
                && value == null) {
            throw new IllegalArgumentException(operator + " requires a non-null comparison value.");
        }
        this.value = value;
    }

    /** Creates a filter with an immutable scalar comparison value. */
    public static Filter comparison(String column, ComparisonOperator operator, Literal value) {
        if (operator == ComparisonOperator.IS_NULL || operator == ComparisonOperator.IS_NOT_NULL) {
            throw new IllegalArgumentException("Use isNull or isNotNull for null checks.");
        }
        return new Filter(column, operator, value);
    }

    /** Creates a null check for {@code column}. */
    public static Filter isNull(String column) {
        return new Filter(column, ComparisonOperator.IS_NULL, null);
    }

    /** Creates a non-null check for {@code column}. */
    public static Filter isNotNull(String column) {
        return new Filter(column, ComparisonOperator.IS_NOT_NULL, null);
    }

    public String column() {
        return column;
    }

    public ComparisonOperator operator() {
        return operator;
    }

    /** Returns the scalar comparison value, or {@code null} for null checks. */
    public Literal value() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Filter)) {
            return false;
        }
        Filter that = (Filter) other;
        return column.equals(that.column)
                && operator == that.operator
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, operator, value);
    }
}
