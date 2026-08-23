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

package org.apache.graphar.io.parquet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.ValueVector;

/** Immutable materialized Parquet values for one physical field. */
final class ParquetValueVector implements ValueVector {
    private final Field field;
    private final List<Object> values;

    ParquetValueVector(Field field, List<?> values) {
        this.field = Objects.requireNonNull(field, "field");
        Objects.requireNonNull(values, "values");
        List<Object> copy = new ArrayList<>(values.size());
        for (Object value : values) {
            copy.add(copyValue(value));
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
        return publicValue(values.get(index));
    }

    private static Object copyValue(Object value) {
        if (value instanceof ByteBuffer) {
            ByteBuffer bytes = ((ByteBuffer) value).asReadOnlyBuffer();
            byte[] copy = new byte[bytes.remaining()];
            bytes.get(copy);
            return copy;
        }
        if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        if (value instanceof List<?>) {
            List<Object> copy = new ArrayList<>(((List<?>) value).size());
            for (Object element : (List<?>) value) {
                copy.add(copyValue(element));
            }
            return Collections.unmodifiableList(copy);
        }
        return value;
    }

    private static Object publicValue(Object value) {
        if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value).asReadOnlyBuffer();
        }
        if (value instanceof List<?>) {
            List<Object> copy = new ArrayList<>(((List<?>) value).size());
            for (Object element : (List<?>) value) {
                copy.add(publicValue(element));
            }
            return Collections.unmodifiableList(copy);
        }
        return value;
    }
}
