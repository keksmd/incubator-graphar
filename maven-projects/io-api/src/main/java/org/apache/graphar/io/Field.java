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

/** A named, typed physical column in a {@link Schema}. */
public final class Field {
    private final String name;
    private final ColumnType type;
    private final boolean nullable;

    public Field(String name, ColumnType type, boolean nullable) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("A field name cannot be blank.");
        }
        this.name = name;
        this.type = Objects.requireNonNull(type, "A field type cannot be null.");
        this.nullable = nullable;
    }

    /** Returns this column name. */
    public String name() {
        return name;
    }

    /** Returns this column's neutral physical type. */
    public ColumnType type() {
        return type;
    }

    /** Returns whether this column may contain null values. */
    public boolean nullable() {
        return nullable;
    }
}
