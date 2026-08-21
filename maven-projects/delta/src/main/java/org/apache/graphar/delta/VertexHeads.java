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
 * An open-addressed map from a vertex number to the newest delta entry recorded for it.
 *
 * <p>The delta is read far more often than a vertex enters it, so this table is sized by the
 * vertices the delta actually touches rather than by the graph. On a projection of tens of millions
 * of vertices patched on a hot set of thousands, a table over the whole vertex space would cost
 * more than the delta it indexes.
 *
 * <p>One writer inserts, any number of readers look up, and neither takes a lock. A slot is
 * published by writing its head entry first and its vertex second: a reader that observes a head
 * observes the entries that head leads to, and a reader that observes a head whose vertex is not
 * yet written keeps probing rather than stopping, so no vertex behind it is lost. The vertex the
 * reader could not see belongs to entries newer than the snapshot it is reading, which that
 * snapshot must not show anyway.
 *
 * <p>Rehashing builds a second table instead of moving this one, so a snapshot keeps reading the
 * table it was taken from.
 */
final class VertexHeads {
    static final int ABSENT = -1;

    private static final long HASH_MIXER = 0x9E3779B97F4A7C15L;
    private static final double LOAD_FACTOR = 0.6;

    private final int[] vertices;
    private final AtomicIntegerArray heads;
    private final int mask;
    private final int growthThreshold;
    private int size;

    VertexHeads(int capacity) {
        int rounded = Integer.highestOneBit(Math.max(16, capacity - 1)) * 2;
        this.vertices = new int[rounded];
        this.heads = new AtomicIntegerArray(rounded);
        this.mask = rounded - 1;
        this.growthThreshold = (int) (rounded * LOAD_FACTOR);
        for (int slot = 0; slot < rounded; slot++) {
            vertices[slot] = ABSENT;
            heads.set(slot, ABSENT);
        }
    }

    /** Returns the newest entry recorded for {@code vertex}, or {@link #ABSENT}. */
    int head(int vertex) {
        int slot = slotOf(vertex, mask);
        while (true) {
            int head = heads.get(slot);
            if (head == ABSENT) {
                return ABSENT;
            }
            if (vertices[slot] == vertex) {
                return head;
            }
            slot = (slot + 1) & mask;
        }
    }

    /** Records {@code entry} as the newest entry of {@code vertex}. */
    void setHead(int vertex, int entry) {
        int slot = slotOf(vertex, mask);
        while (heads.get(slot) != ABSENT) {
            if (vertices[slot] == vertex) {
                heads.set(slot, entry);
                return;
            }
            slot = (slot + 1) & mask;
        }
        heads.set(slot, entry);
        vertices[slot] = vertex;
        size++;
    }

    /** Returns the number of vertices the delta has entries for. */
    int size() {
        return size;
    }

    /** Reports whether one more vertex should go into a larger table. */
    boolean isFull() {
        return size >= growthThreshold;
    }

    /** Returns a table of twice the capacity holding everything this one holds. */
    VertexHeads grown() {
        VertexHeads grown = new VertexHeads((mask + 1) * 2);
        for (int slot = 0; slot <= mask; slot++) {
            int vertex = vertices[slot];
            if (vertex != ABSENT) {
                grown.setHead(vertex, heads.get(slot));
            }
        }
        return grown;
    }

    private static int slotOf(int vertex, int mask) {
        return (int) ((vertex * HASH_MIXER) >>> 40) & mask;
    }
}
