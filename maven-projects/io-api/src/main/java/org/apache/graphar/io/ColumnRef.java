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
 * An unresolved reference to one top-level column of a physical file, addressed by its exact name.
 *
 * <p>A request is built before the file it targets is opened, so a reference can only name a
 * column; binding it to a position and a {@link ColumnType} happens when a physical reader resolves
 * it against the file {@link Schema} with {@link Schema#resolve(ColumnRef)}. That resolution is
 * exact: names are compared verbatim, a name that is absent from the schema is an error, and a name
 * that matches more than one field is an error rather than a first-match guess. A reference carries
 * no qualifier because the physical boundary reads exactly one file and one schema; mapping a
 * qualified logical property onto a physical column is the caller's job.
 */
public final class ColumnRef {
    private final String name;

    private ColumnRef(String name) {
        this.name = name;
    }

    /** References the top-level column called exactly {@code name}. */
    public static ColumnRef of(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("A column name cannot be blank.");
        }
        return new ColumnRef(name);
    }

    /** Returns the exact column name this reference resolves by. */
    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ColumnRef && name.equals(((ColumnRef) other).name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
