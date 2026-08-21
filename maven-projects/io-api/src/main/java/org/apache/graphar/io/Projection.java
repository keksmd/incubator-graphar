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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** An ordered set of requested output columns. */
public final class Projection {
    private static final Projection ALL_COLUMNS = new Projection(true, List.of());

    private final boolean allColumns;
    private final List<String> columns;

    private Projection(boolean allColumns, List<String> columns) {
        this.allColumns = allColumns;
        this.columns = columns;
    }

    /** Requests every available column. */
    public static Projection all() {
        return ALL_COLUMNS;
    }

    /** Requests the supplied columns in order. */
    public static Projection of(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("A projection must contain at least one column.");
        }
        List<String> copy = new ArrayList<>(columns.size());
        Set<String> names = new HashSet<>();
        for (String column : columns) {
            if (column == null || column.isBlank()) {
                throw new IllegalArgumentException("Projection column names cannot be blank.");
            }
            if (!names.add(column)) {
                throw new IllegalArgumentException(
                        "Projection contains duplicate column: " + column);
            }
            copy.add(column);
        }
        return new Projection(false, List.copyOf(copy));
    }

    /** Returns whether this projection requests every available column. */
    public boolean isAllColumns() {
        return allColumns;
    }

    /** Returns the requested columns, or an empty list when all columns are requested. */
    public List<String> columns() {
        return columns;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Projection)) {
            return false;
        }
        Projection that = (Projection) other;
        return allColumns == that.allColumns && columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(allColumns, columns);
    }
}
