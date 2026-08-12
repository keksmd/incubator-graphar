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

package org.apache.graphar.writer;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** One topology row and its edge properties, aligned by its physical GraphAr row position. */
public final class EdgeRecord {
    private final long source;
    private final long destination;
    private final Map<String, Object> properties;

    /** Creates an edge with immutable scalar property values keyed by GraphAr property name. */
    public EdgeRecord(long source, long destination, Map<String, Object> properties) {
        this.source = source;
        this.destination = destination;
        // Edge properties may be nullable. Map.copyOf would reject those values even when
        // EdgeInfo explicitly declares the corresponding property nullable.
        this.properties =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(
                                Objects.requireNonNull(properties, "Properties cannot be null.")));
    }

    public long source() {
        return source;
    }

    public long destination() {
        return destination;
    }

    public Map<String, Object> properties() {
        return properties;
    }
}
