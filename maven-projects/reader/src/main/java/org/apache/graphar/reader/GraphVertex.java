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

package org.apache.graphar.reader;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** One GraphAr vertex identified by its internal index, with the selected property values. */
public final class GraphVertex {
    private final long id;
    private final Map<String, Object> properties;

    GraphVertex(long id, Map<String, Object> properties) {
        if (id < 0) {
            throw new IllegalArgumentException("GraphAr vertex IDs must be non-negative.");
        }
        this.id = id;
        this.properties =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(
                                Objects.requireNonNull(
                                        properties, "Vertex properties cannot be null.")));
    }

    /** Returns the internal GraphAr vertex index. */
    public long id() {
        return id;
    }

    /** Returns the selected property values in declaration order. */
    public Map<String, Object> properties() {
        return properties;
    }

    /** Returns one selected property value, or {@code null} when it was not projected. */
    public Object property(String name) {
        return properties.get(name);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GraphVertex)) {
            return false;
        }
        GraphVertex that = (GraphVertex) other;
        return id == that.id && properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, properties);
    }

    @Override
    public String toString() {
        return "GraphVertex{id=" + id + ", properties=" + properties + '}';
    }
}
