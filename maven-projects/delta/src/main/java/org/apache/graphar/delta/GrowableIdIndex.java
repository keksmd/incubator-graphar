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

package org.apache.graphar.delta;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * An open-addressed map from an application identifier to a vertex number that grows as identifiers
 * arrive and stays readable while it does.
 *
 * <p>The projection resolves identifiers through a table built once from a finished dataset. A
 * delta cannot: the identifiers it has to name are the ones that were not in the dataset when the
 * projection was built, and they arrive one request at a time.
 *
 * <p>One writer inserts, any number of readers look up, and neither takes a lock. A slot is
 * published by writing its value first and its key second, so a reader that observes a value
 * observes every write the writer made before it, and a reader that observes a value without its
 * key simply keeps probing. The value of an identifier is never replaced, so a reader that finds a
 * key finds the number that identifier will keep.
 *
 * <p>Rehashing does not touch the table being read: it builds a second one, which the delta
 * publishes in place of this one. A snapshot taken before the rehash keeps reading the table it was
 * taken from, which is complete for every identifier that existed when it was taken.
 */
final class GrowableIdIndex {
    static final int ABSENT = -1;

    private static final long HASH_MIXER = 0x9E3779B97F4A7C15L;
    private static final double LOAD_FACTOR = 0.6;
    private static final int MINIMUM_CAPACITY = 16;

    private final String[] keys;
    private final AtomicIntegerArray values;
    private final int mask;
    private final int growthThreshold;
    private int size;

    GrowableIdIndex() {
        this(MINIMUM_CAPACITY);
    }

    private GrowableIdIndex(int capacity) {
        this.keys = new String[capacity];
        this.values = new AtomicIntegerArray(capacity);
        this.mask = capacity - 1;
        this.growthThreshold = (int) (capacity * LOAD_FACTOR);
        for (int slot = 0; slot < capacity; slot++) {
            values.set(slot, ABSENT);
        }
    }

    /** Returns the vertex number stored for {@code key}, or {@link #ABSENT}. */
    int lookup(String key) {
        int slot = slotOf(key, mask);
        while (true) {
            int value = values.get(slot);
            if (value == ABSENT) {
                return ABSENT;
            }
            if (key.equals(keys[slot])) {
                return value;
            }
            slot = (slot + 1) & mask;
        }
    }

    /**
     * Stores {@code value} under {@code key} when the key is new and returns it, or returns the
     * value already stored without replacing it. Only the writer that owns this index may call it.
     */
    int putIfAbsent(String key, int value) {
        int slot = slotOf(key, mask);
        while (values.get(slot) != ABSENT) {
            if (key.equals(keys[slot])) {
                return values.get(slot);
            }
            slot = (slot + 1) & mask;
        }
        values.set(slot, value);
        keys[slot] = key;
        size++;
        return value;
    }

    /** Returns the number of identifiers stored. */
    int size() {
        return size;
    }

    /** Reports whether one more identifier should go into a larger table. */
    boolean isFull() {
        return size >= growthThreshold;
    }

    /** Returns a table of twice the capacity holding everything this one holds. */
    GrowableIdIndex grown() {
        GrowableIdIndex grown = new GrowableIdIndex((mask + 1) * 2);
        for (int slot = 0; slot <= mask; slot++) {
            String key = keys[slot];
            if (key != null) {
                grown.putIfAbsent(key, values.get(slot));
            }
        }
        return grown;
    }

    private static int slotOf(String key, int mask) {
        return (int) ((key.hashCode() * HASH_MIXER) >>> 40) & mask;
    }
}
