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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An inspectable row predicate: a column comparison, or a boolean combination of predicates. The
 * hierarchy is closed; a physical reader walks it with a {@link Visitor} and gets one call per node
 * kind, so a reader that does not push a node down still evaluates it as fallback.
 *
 * <p>A physical reader must validate a comparison literal against the column's {@link ColumnType}:
 * BOOLEAN uses Boolean; integer and floating kinds use their matching boxed Java types; STRING uses
 * String; DATE uses LocalDate; and TIMESTAMP_MILLIS uses millisecond-precise Instant. Other types
 * cannot be compared by this contract. Comparison operands must have the same declared type; a type
 * mismatch is invalid rather than a coercion. Null values never match a comparison, including
 * NOT_EQUAL; use {@link #isNull} or {@link #isNotNull} for null tests. Ordered STRING comparisons
 * use {@link String#compareTo(String)}, and DATE and TIMESTAMP_MILLIS use their natural ordering.
 * Readers must reject an invalid comparison before returning a result, whether the filter is pushed
 * down or evaluated as fallback.
 *
 * <p>Boolean nodes use two-valued logic over the comparison results above: a comparison on a null
 * value is false, so {@code not(equal(c, v))} keeps rows where {@code c} is null while {@code
 * notEqual(c, v)} does not.
 */
public abstract class Filter {
    Filter() {}

    /** Keeps rows whose {@code column} equals {@code value}. */
    public static Filter equal(ColumnRef column, Literal value) {
        return comparison(column, ComparisonOperator.EQUAL, value);
    }

    /** Keeps non-null rows whose {@code column} differs from {@code value}. */
    public static Filter notEqual(ColumnRef column, Literal value) {
        return comparison(column, ComparisonOperator.NOT_EQUAL, value);
    }

    /** Keeps rows whose {@code column} is strictly less than {@code value}. */
    public static Filter lessThan(ColumnRef column, Literal value) {
        return comparison(column, ComparisonOperator.LESS_THAN, value);
    }

    /** Keeps rows whose {@code column} is less than or equal to {@code value}. */
    public static Filter lessThanOrEqual(ColumnRef column, Literal value) {
        return comparison(column, ComparisonOperator.LESS_THAN_OR_EQUAL, value);
    }

    /** Keeps rows whose {@code column} is strictly greater than {@code value}. */
    public static Filter greaterThan(ColumnRef column, Literal value) {
        return comparison(column, ComparisonOperator.GREATER_THAN, value);
    }

    /** Keeps rows whose {@code column} is greater than or equal to {@code value}. */
    public static Filter greaterThanOrEqual(ColumnRef column, Literal value) {
        return comparison(column, ComparisonOperator.GREATER_THAN_OR_EQUAL, value);
    }

    /** Keeps rows whose {@code column} is null. */
    public static Filter isNull(ColumnRef column) {
        return new Comparison(column, ComparisonOperator.IS_NULL, null);
    }

    /** Keeps rows whose {@code column} is not null. */
    public static Filter isNotNull(ColumnRef column) {
        return new Comparison(column, ComparisonOperator.IS_NOT_NULL, null);
    }

    /** Keeps rows that satisfy every operand; nested conjunctions are flattened. */
    public static Filter and(Filter first, Filter second, Filter... rest) {
        return and(operands(first, second, rest));
    }

    /** Keeps rows that satisfy every operand; nested conjunctions are flattened. */
    public static Filter and(List<Filter> operands) {
        List<Filter> flat = flatten(operands, And.class);
        return flat.size() == 1 ? flat.get(0) : new And(flat);
    }

    /** Keeps rows that satisfy at least one operand; nested disjunctions are flattened. */
    public static Filter or(Filter first, Filter second, Filter... rest) {
        return or(operands(first, second, rest));
    }

    /** Keeps rows that satisfy at least one operand; nested disjunctions are flattened. */
    public static Filter or(List<Filter> operands) {
        List<Filter> flat = flatten(operands, Or.class);
        return flat.size() == 1 ? flat.get(0) : new Or(flat);
    }

    /** Keeps rows that do not satisfy {@code operand}; a double negation is removed. */
    public static Filter not(Filter operand) {
        Objects.requireNonNull(operand, "A filter operand cannot be null.");
        return operand instanceof Not ? ((Not) operand).operand() : new Not(operand);
    }

    /** Returns {@code and(this, other)}. */
    public final Filter and(Filter other) {
        return and(this, other);
    }

    /** Returns {@code or(this, other)}. */
    public final Filter or(Filter other) {
        return or(this, other);
    }

    /** Returns {@code not(this)}. */
    public final Filter negate() {
        return not(this);
    }

    /** Dispatches on this node's kind. */
    public abstract <R> R accept(Visitor<R> visitor);

    private static Filter comparison(ColumnRef column, ComparisonOperator operator, Literal value) {
        return new Comparison(
                column,
                operator,
                Objects.requireNonNull(value, "A comparison value cannot be null."));
    }

    private static List<Filter> operands(Filter first, Filter second, Filter[] rest) {
        List<Filter> operands = new ArrayList<>(2 + (rest == null ? 0 : rest.length));
        operands.add(first);
        operands.add(second);
        if (rest != null) {
            operands.addAll(Arrays.asList(rest));
        }
        return operands;
    }

    private static List<Filter> flatten(List<Filter> operands, Class<? extends Filter> same) {
        if (operands == null || operands.isEmpty()) {
            throw new IllegalArgumentException("A boolean filter needs at least one operand.");
        }
        List<Filter> flat = new ArrayList<>(operands.size());
        for (Filter operand : operands) {
            Objects.requireNonNull(operand, "A filter operand cannot be null.");
            if (same.isInstance(operand)) {
                flat.addAll(((Junction) operand).operands());
            } else {
                flat.add(operand);
            }
        }
        return List.copyOf(flat);
    }

    /** One callback per node kind; a reader implements all four. */
    public interface Visitor<R> {
        R comparison(Comparison filter);

        R and(And filter);

        R or(Or filter);

        R not(Not filter);
    }

    /** A single column compared against a literal, or tested for null. */
    public static final class Comparison extends Filter {
        private final ColumnRef column;
        private final ComparisonOperator operator;
        private final Literal value;

        private Comparison(ColumnRef column, ComparisonOperator operator, Literal value) {
            this.column = Objects.requireNonNull(column, "A filter column cannot be null.");
            this.operator = Objects.requireNonNull(operator, "Filter operator cannot be null.");
            this.value = value;
        }

        /** Returns the column this comparison inspects. */
        public ColumnRef column() {
            return column;
        }

        /** Returns the comparison applied. */
        public ComparisonOperator operator() {
            return operator;
        }

        /** Returns the comparison value, which is absent for null checks. */
        public Optional<Literal> value() {
            return Optional.ofNullable(value);
        }

        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.comparison(this);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Comparison)) {
                return false;
            }
            Comparison that = (Comparison) other;
            return column.equals(that.column)
                    && operator == that.operator
                    && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(column, operator, value);
        }

        @Override
        public String toString() {
            return value == null
                    ? column + " " + operator
                    : column + " " + operator + " " + value.value();
        }
    }

    /** A boolean node over two or more operands. */
    public abstract static class Junction extends Filter {
        private final List<Filter> operands;

        private Junction(List<Filter> operands) {
            this.operands = operands;
        }

        /** Returns the immutable operands in the given order. */
        public final List<Filter> operands() {
            return operands;
        }

        @Override
        public final boolean equals(Object other) {
            return other != null
                    && getClass() == other.getClass()
                    && operands.equals(((Junction) other).operands);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(getClass(), operands);
        }
    }

    /** Every operand must hold. */
    public static final class And extends Junction {
        private And(List<Filter> operands) {
            super(operands);
        }

        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.and(this);
        }

        @Override
        public String toString() {
            return "AND" + operands();
        }
    }

    /** At least one operand must hold. */
    public static final class Or extends Junction {
        private Or(List<Filter> operands) {
            super(operands);
        }

        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.or(this);
        }

        @Override
        public String toString() {
            return "OR" + operands();
        }
    }

    /** The operand must not hold. */
    public static final class Not extends Filter {
        private final Filter operand;

        private Not(Filter operand) {
            this.operand = operand;
        }

        /** Returns the negated predicate. */
        public Filter operand() {
            return operand;
        }

        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.not(this);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Not && operand.equals(((Not) other).operand);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Not.class, operand);
        }

        @Override
        public String toString() {
            return "NOT[" + operand + "]";
        }
    }
}
