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
import java.util.Optional;

/** A recursive, format-neutral type used in a physical {@link Schema}. */
public final class ColumnType {
    /** The scalar and container kinds represented by this contract. */
    public enum Kind {
        BOOLEAN,
        INT8,
        INT16,
        INT32,
        INT64,
        FLOAT32,
        FLOAT64,
        STRING,
        BINARY,
        DATE,
        TIMESTAMP_MILLIS,
        LIST
    }

    private final Kind kind;
    private final ColumnType elementType;

    private ColumnType(Kind kind, ColumnType elementType) {
        this.kind = kind;
        this.elementType = elementType;
    }

    /** Creates a scalar type. */
    public static ColumnType of(Kind kind) {
        Objects.requireNonNull(kind, "A column type kind cannot be null.");
        if (kind == Kind.LIST) {
            throw new IllegalArgumentException("Use listOf to create a list type.");
        }
        return new ColumnType(kind, null);
    }

    /** Creates a list whose values have {@code elementType}. */
    public static ColumnType listOf(ColumnType elementType) {
        return new ColumnType(
                Kind.LIST, Objects.requireNonNull(elementType, "Element type cannot be null."));
    }

    /** Returns this type's kind. */
    public Kind kind() {
        return kind;
    }

    /** Returns the list element type, or empty for scalar types. */
    public Optional<ColumnType> elementType() {
        return Optional.ofNullable(elementType);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ColumnType)) {
            return false;
        }
        ColumnType that = (ColumnType) other;
        return kind == that.kind && Objects.equals(elementType, that.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, elementType);
    }
}
