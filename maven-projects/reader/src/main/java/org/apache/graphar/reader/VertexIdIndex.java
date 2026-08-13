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

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.info.VertexInfo;

/**
 * Resolves an application-visible vertex identifier to the dense GraphAr vertex index of one vertex
 * type.
 *
 * <p>GraphAr addresses vertices by a dense index assigned by physical row position, while callers
 * usually hold a natural identifier stored as a vertex property. The format does not persist that
 * mapping, so an application serving lookups by natural identifier has to build it once per
 * projection. This index is that artifact: a single projected scan of the identifier column
 * produces an immutable open-addressed table that answers lookups without further I/O.
 *
 * <p>Identifiers are compared by their string form, because the mapping exists to serve string
 * request parameters. A numeric identifier column is therefore matched by its decimal text.
 *
 * <p>The table is sized from the declared vertex count and never rehashes. Heap cost is one {@code
 * String} reference and one {@code long} per slot, with slots at roughly 1.6 times the vertex
 * count, plus the identifier strings themselves.
 *
 * <p>Instances are immutable after construction and safe to share between threads.
 */
public final class VertexIdIndex {
    /** Returned by {@link #lookup(String)} when the identifier is not present. */
    public static final long ABSENT = -1L;

    private static final double LOAD_FACTOR = 0.6;
    private static final int MINIMUM_CAPACITY = 16;
    private static final long HASH_MIXER = 0x9E3779B97F4A7C15L;

    private final String idProperty;
    private final String[] keys;
    private final long[] indexes;
    private final int mask;
    private final int size;

    private VertexIdIndex(String idProperty, String[] keys, long[] indexes, int size) {
        this.idProperty = idProperty;
        this.keys = keys;
        this.indexes = indexes;
        this.mask = keys.length - 1;
        this.size = size;
    }

    /**
     * Builds the index over the single primary property declared by this vertex type.
     *
     * @throws IllegalArgumentException when the vertex type declares no primary property or more
     *     than one
     */
    public static VertexIdIndex build(VertexReader reader) throws IOException {
        Objects.requireNonNull(reader, "Vertex reader cannot be null.");
        return build(reader, primaryProperty(reader.vertexInfo()));
    }

    /**
     * Builds the index over the named property with one projected scan of that column.
     *
     * @throws IllegalArgumentException when the property is undeclared, holds a null value, or
     *     repeats an identifier already seen
     */
    public static VertexIdIndex build(VertexReader reader, String idProperty) throws IOException {
        Objects.requireNonNull(reader, "Vertex reader cannot be null.");
        Objects.requireNonNull(idProperty, "Identifier property cannot be null.");
        if (!reader.vertexInfo().hasProperty(idProperty)) {
            throw new IllegalArgumentException(
                    "Vertex type "
                            + reader.vertexInfo().getType()
                            + " does not declare property "
                            + idProperty
                            + '.');
        }
        long vertexCount = reader.vertexCount();
        int capacity = capacityFor(vertexCount);
        String[] keys = new String[capacity];
        long[] indexes = new long[capacity];
        int size = 0;
        try (VertexPropertyCursor cursor = reader.scan(List.of(idProperty))) {
            while (cursor.next()) {
                GraphVertex vertex = cursor.vertex();
                Object value = vertex.property(idProperty);
                if (value == null) {
                    throw new IllegalArgumentException(
                            "Identifier property "
                                    + idProperty
                                    + " is null at vertex "
                                    + vertex.id()
                                    + '.');
                }
                insert(keys, indexes, String.valueOf(value), vertex.id(), idProperty);
                size++;
            }
        }
        return new VertexIdIndex(idProperty, keys, indexes, size);
    }

    /**
     * Returns the dense GraphAr vertex index of the identifier, or {@link #ABSENT} when the
     * identifier belongs to no vertex of this type.
     */
    public long lookup(String externalId) {
        Objects.requireNonNull(externalId, "Identifier cannot be null.");
        int slot = slotOf(externalId, mask);
        while (keys[slot] != null) {
            if (keys[slot].equals(externalId)) {
                return indexes[slot];
            }
            slot = (slot + 1) & mask;
        }
        return ABSENT;
    }

    /** Reports whether the identifier belongs to a vertex of this type. */
    public boolean contains(String externalId) {
        return lookup(externalId) != ABSENT;
    }

    /** Returns the number of indexed vertices. */
    public int size() {
        return size;
    }

    /** Returns the property whose values this index resolves. */
    public String idProperty() {
        return idProperty;
    }

    private static void insert(
            String[] keys, long[] indexes, String key, long vertexIndex, String idProperty) {
        int mask = keys.length - 1;
        int slot = slotOf(key, mask);
        while (keys[slot] != null) {
            if (keys[slot].equals(key)) {
                throw new IllegalArgumentException(
                        "Identifier property "
                                + idProperty
                                + " repeats value "
                                + key
                                + " at vertices "
                                + indexes[slot]
                                + " and "
                                + vertexIndex
                                + '.');
            }
            slot = (slot + 1) & mask;
        }
        keys[slot] = key;
        indexes[slot] = vertexIndex;
    }

    private static int slotOf(String key, int mask) {
        long mixed = key.hashCode() * HASH_MIXER;
        return (int) ((mixed >>> 32) ^ mixed) & mask;
    }

    private static int capacityFor(long vertexCount) {
        if (vertexCount < 0) {
            throw new IllegalArgumentException("Vertex count cannot be negative.");
        }
        long required = (long) Math.ceil(vertexCount / LOAD_FACTOR) + 1;
        if (required > (1L << 30)) {
            throw new IllegalArgumentException(
                    "Vertex type has too many vertices for a single in-memory identifier index: "
                            + vertexCount
                            + '.');
        }
        int capacity = MINIMUM_CAPACITY;
        while (capacity < required) {
            capacity <<= 1;
        }
        return capacity;
    }

    private static String primaryProperty(VertexInfo vertexInfo) {
        String primary = null;
        for (int group = 0; group < vertexInfo.getPropertyGroupNum(); group++) {
            PropertyGroup propertyGroup = vertexInfo.getPropertyGroupByIndex(group);
            for (Property property : propertyGroup.getPropertyList()) {
                if (!property.isPrimary()) {
                    continue;
                }
                if (primary != null) {
                    throw new IllegalArgumentException(
                            "Vertex type "
                                    + vertexInfo.getType()
                                    + " declares more than one primary property: "
                                    + primary
                                    + " and "
                                    + property.getName()
                                    + '.');
                }
                primary = property.getName();
            }
        }
        if (primary == null) {
            throw new IllegalArgumentException(
                    "Vertex type "
                            + vertexInfo.getType()
                            + " declares no primary property; name the identifier property"
                            + " explicitly.");
        }
        return primary;
    }
}
