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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@link ValueVector} over boxed values held in memory.
 *
 * <p>This is the vector a producer uses when it assembles a batch from Java objects rather than
 * decoding one from a file. Values are snapshotted at construction and list values are held as
 * unmodifiable copies, so neither the producer nor a consumer can mutate the vector afterwards.
 */
public final class ObjectValueVector implements ValueVector {
    private final Field field;
    private final List<Object> values;

    public ObjectValueVector(Field field, List<?> values) {
        this.field = Objects.requireNonNull(field, "A vector field cannot be null.");
        Objects.requireNonNull(values, "Vector values cannot be null.");
        List<Object> copy = new ArrayList<>(values.size());
        for (Object value : values) {
            copy.add(value instanceof List ? List.copyOf((List<?>) value) : value);
        }
        this.values = Collections.unmodifiableList(copy);
    }

    @Override
    public Field field() {
        return field;
    }

    @Override
    public int valueCount() {
        return values.size();
    }

    @Override
    public boolean isNull(int index) {
        return values.get(index) == null;
    }

    @Override
    public Object getObject(int index) {
        return values.get(index);
    }
}
