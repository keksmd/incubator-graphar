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

/** An ordered, immutable mapping of physical batch columns to neutral field definitions. */
public final class Schema {
    private final List<Field> fields;

    public Schema(List<Field> fields) {
        Objects.requireNonNull(fields, "Schema fields cannot be null.");
        List<Field> copy = new ArrayList<>(fields.size());
        Set<String> names = new HashSet<>();
        for (Field field : fields) {
            Field nonNullField = Objects.requireNonNull(field, "A schema field cannot be null.");
            if (!names.add(nonNullField.name())) {
                throw new IllegalArgumentException(
                        "Schema contains duplicate field: " + nonNullField.name());
            }
            copy.add(nonNullField);
        }
        this.fields = List.copyOf(copy);
    }

    /** Returns immutable fields in physical column order. */
    public List<Field> fields() {
        return fields;
    }
}
