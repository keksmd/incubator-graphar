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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/** A recursive, Arrow-shaped logical type used in a physical {@link Schema}. */
public final class ColumnType {
    /** Scalar, fixed-width, and nested shapes that a physical format may describe. */
    public enum Kind {
        BOOLEAN,
        INT8,
        INT16,
        INT32,
        INT64,
        UINT8,
        UINT16,
        UINT32,
        UINT64,
        FLOAT32,
        FLOAT64,
        STRING,
        BINARY,
        DATE,
        TIMESTAMP_MILLIS,
        FIXED_SIZE_BINARY,
        DECIMAL,
        LIST,
        FIXED_SIZE_LIST,
        STRUCT,
        MAP
    }

    private final Kind kind;
    private final List<Field> children;
    private final int fixedSize;
    private final int precision;
    private final int scale;

    private ColumnType(Kind kind, List<Field> children, int fixedSize, int precision, int scale) {
        this.kind = kind;
        this.children = children;
        this.fixedSize = fixedSize;
        this.precision = precision;
        this.scale = scale;
    }

    /** Creates one of the scalar kinds. */
    public static ColumnType of(Kind kind) {
        Objects.requireNonNull(kind, "A column type kind cannot be null.");
        if (!isScalar(kind)) {
            throw new IllegalArgumentException("Use a dedicated factory for " + kind + ".");
        }
        return new ColumnType(kind, List.of(), 0, 0, 0);
    }

    /** Creates a list whose anonymous element has {@code elementType}. */
    public static ColumnType listOf(ColumnType elementType) {
        return listOfElement(new Field("element", elementType, true));
    }

    /** Creates a list whose element field retains its physical name and nullability. */
    public static ColumnType listOfElement(Field elementField) {
        return collection(Kind.LIST, elementField, 0);
    }

    /** Creates a fixed-size list whose every value contains exactly {@code listSize} elements. */
    public static ColumnType fixedSizeListOf(Field elementField, int listSize) {
        if (listSize <= 0) {
            throw new IllegalArgumentException("A fixed-size list must have a positive size.");
        }
        return collection(Kind.FIXED_SIZE_LIST, elementField, listSize);
    }

    /** Creates a struct with the supplied named children in physical order. */
    public static ColumnType structOf(List<Field> fields) {
        return new ColumnType(Kind.STRUCT, copyFields(fields), 0, 0, 0);
    }

    /** Creates a map with the physical key and value fields in that order. */
    public static ColumnType mapOf(Field key, Field value) {
        Objects.requireNonNull(key, "A map key field cannot be null.");
        if (key.nullable()) {
            throw new IllegalArgumentException("A map key field cannot be nullable.");
        }
        return new ColumnType(
                Kind.MAP,
                List.of(key, Objects.requireNonNull(value, "A map value field cannot be null.")),
                0,
                0,
                0);
    }

    /** Creates a fixed-width binary value with exactly {@code byteWidth} bytes. */
    public static ColumnType fixedSizeBinary(int byteWidth) {
        if (byteWidth <= 0) {
            throw new IllegalArgumentException("Fixed-size binary width must be positive.");
        }
        return new ColumnType(Kind.FIXED_SIZE_BINARY, List.of(), byteWidth, 0, 0);
    }

    /** Creates a decimal whose scale is between zero and its positive precision. */
    public static ColumnType decimal(int precision, int scale) {
        if (precision <= 0 || scale < 0 || scale > precision) {
            throw new IllegalArgumentException(
                    "Decimal requires 0 <= scale <= positive precision.");
        }
        return new ColumnType(Kind.DECIMAL, List.of(), 0, precision, scale);
    }

    /** Returns this type's shape. */
    public Kind kind() {
        return kind;
    }

    /** Returns child fields in physical order. Scalar types have no children. */
    public List<Field> children() {
        return children;
    }

    /** Returns the list element type for a list shape, or empty for all other shapes. */
    public Optional<ColumnType> elementType() {
        return (kind == Kind.LIST || kind == Kind.FIXED_SIZE_LIST)
                ? Optional.of(children.get(0).type())
                : Optional.empty();
    }

    /** Returns a fixed binary width or fixed-list size when one applies. */
    public OptionalInt fixedSize() {
        return fixedSize == 0 ? OptionalInt.empty() : OptionalInt.of(fixedSize);
    }

    /** Returns decimal precision for a decimal type. */
    public OptionalInt precision() {
        return precision == 0 ? OptionalInt.empty() : OptionalInt.of(precision);
    }

    /** Returns decimal scale for a decimal type. */
    public OptionalInt scale() {
        return kind == Kind.DECIMAL ? OptionalInt.of(scale) : OptionalInt.empty();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ColumnType)) {
            return false;
        }
        ColumnType that = (ColumnType) other;
        return kind == that.kind
                && fixedSize == that.fixedSize
                && precision == that.precision
                && scale == that.scale
                && children.equals(that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, children, fixedSize, precision, scale);
    }

    private static ColumnType collection(Kind kind, Field elementField, int fixedSize) {
        return new ColumnType(
                kind,
                List.of(
                        Objects.requireNonNull(
                                elementField, "A list element field cannot be null.")),
                fixedSize,
                0,
                0);
    }

    private static List<Field> copyFields(List<Field> fields) {
        Objects.requireNonNull(fields, "Child fields cannot be null.");
        for (Field field : fields) {
            Objects.requireNonNull(field, "A child field cannot be null.");
        }
        return List.copyOf(fields);
    }

    private static boolean isScalar(Kind kind) {
        return kind != Kind.FIXED_SIZE_BINARY
                && kind != Kind.DECIMAL
                && kind != Kind.LIST
                && kind != Kind.FIXED_SIZE_LIST
                && kind != Kind.STRUCT
                && kind != Kind.MAP;
    }
}
