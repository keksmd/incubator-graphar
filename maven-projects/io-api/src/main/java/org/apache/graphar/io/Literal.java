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

import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/** An immutable scalar used by a {@link Filter} comparison. */
public final class Literal {
    private final Object value;

    private Literal(Object value) {
        this.value = value;
    }

    /**
     * Wraps a supported immutable scalar: {@link Boolean}, numeric boxed primitives, {@link
     * String}, {@link LocalDate}, or millisecond-precise {@link Instant}.
     */
    public static Literal of(Object value) {
        Objects.requireNonNull(value, "A literal value cannot be null.");
        if (!(value instanceof Boolean)
                && !(value instanceof Byte)
                && !(value instanceof Short)
                && !(value instanceof Integer)
                && !(value instanceof Long)
                && !(value instanceof Float)
                && !(value instanceof Double)
                && !(value instanceof String)
                && !(value instanceof LocalDate)
                && !(value instanceof Instant)) {
            throw new IllegalArgumentException(
                    "A literal must be a Boolean, numeric boxed primitive, String, LocalDate, or Instant.");
        }
        if (value instanceof Float && !Float.isFinite((Float) value)) {
            throw new IllegalArgumentException("A floating point literal must be finite.");
        }
        if (value instanceof Double && !Double.isFinite((Double) value)) {
            throw new IllegalArgumentException("A floating point literal must be finite.");
        }
        if (value instanceof Instant && ((Instant) value).getNano() % 1_000_000 != 0) {
            throw new IllegalArgumentException(
                    "An Instant literal must have millisecond precision.");
        }
        return new Literal(value);
    }

    /** Returns this literal's immutable scalar value. */
    public Object value() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Literal && value.equals(((Literal) other).value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
