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

package org.apache.graphar.integration.ignite;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.graphar.reader.EdgeCursor;
import org.apache.graphar.reader.OrderedSourceEdgeReader;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Serves immutable GraphAr ordered-source topology as compact CSR shards in an Ignite off-heap
 * cache.
 *
 * <p>Each load is identified by a caller-owned immutable snapshot ID. A writer loads all shards for
 * one ID before making that ID available to callers; callers never observe a mixed snapshot. Ignite
 * configuration, node lifecycle, credentials, and retention remain application-owned.
 */
public final class IgniteCsrStore {
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_BYTES = Integer.BYTES * 3 + Long.BYTES;

    private final Ignite ignite;
    private final String cacheName;
    private final int verticesPerShard;

    /**
     * Creates or opens an application-named partitioned cache. The named data region must be
     * configured by the caller on the supplied Ignite node.
     */
    public IgniteCsrStore(
            Ignite ignite, String cacheName, String dataRegionName, int verticesPerShard) {
        this(ignite, cacheName, dataRegionName, verticesPerShard, 0);
    }

    /**
     * Creates or opens an application-named partitioned cache with the requested number of backup
     * copies. Use at least one backup for a production service that must tolerate a primary-node
     * loss while publishing a snapshot.
     */
    public IgniteCsrStore(
            Ignite ignite,
            String cacheName,
            String dataRegionName,
            int verticesPerShard,
            int backups) {
        this.ignite = Objects.requireNonNull(ignite, "Ignite cannot be null.");
        this.cacheName = requireNonBlank(cacheName, "Cache name");
        requireNonBlank(dataRegionName, "Data region name");
        if (verticesPerShard <= 0 || backups < 0) {
            throw new IllegalArgumentException(
                    "Vertices per CSR shard must be positive and backups cannot be negative.");
        }
        this.verticesPerShard = verticesPerShard;
        ignite.getOrCreateCache(
                new CacheConfiguration<ShardKey, byte[]>(cacheName)
                        .setDataRegionName(dataRegionName)
                        .setCacheMode(CacheMode.PARTITIONED)
                        .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                        .setBackups(backups)
                        .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));
    }

    /** Returns the configured logical source-vertex span of every CSR shard. */
    public int verticesPerShard() {
        return verticesPerShard;
    }

    /**
     * Streams a GraphAr ordered-by-source snapshot once and writes compact immutable CSR shards.
     * Existing shards for this exact snapshot ID are rejected, preventing accidental mixed loads.
     */
    public LoadResult load(String snapshotId, OrderedSourceEdgeReader reader) throws IOException {
        requireNonBlank(snapshotId, "Snapshot ID");
        Objects.requireNonNull(reader, "Ordered source reader cannot be null.");
        long vertexCount = reader.vertexCount();
        long edgeCount = reader.edgeCount();
        long shardCount = divideRoundUp(vertexCount, verticesPerShard);
        if (shardCount > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Too many CSR shards: " + shardCount);
        }

        IgniteCache<ShardKey, byte[]> cache = cache();
        if (cache.containsKey(ShardKey.manifest(snapshotId))) {
            throw new IllegalStateException("CSR snapshot is already published: " + snapshotId);
        }
        long loadedEdges = 0;
        int shardIndex = 0;
        long previousSource = -1;
        CsrBuilder builder = shardCount == 0 ? null : newBuilder(vertexCount, shardIndex);
        try (EdgeCursor cursor = reader.scanEdges()) {
            while (cursor.next()) {
                long source = cursor.source();
                if (source < previousSource || source >= vertexCount || builder == null) {
                    throw new IllegalArgumentException(
                            "GraphAr source scan is not sorted and in bounds.");
                }
                while (source >= builder.vertexEnd()) {
                    loadedEdges =
                            Math.addExact(
                                    loadedEdges, publish(cache, snapshotId, shardIndex, builder));
                    shardIndex++;
                    builder = newBuilder(vertexCount, shardIndex);
                }
                builder.add(source, cursor.destination());
                previousSource = source;
            }
        }
        while (shardIndex < shardCount) {
            loadedEdges =
                    Math.addExact(loadedEdges, publish(cache, snapshotId, shardIndex, builder));
            shardIndex++;
            builder = shardIndex == shardCount ? null : newBuilder(vertexCount, shardIndex);
        }
        if (loadedEdges != edgeCount) {
            throw new IllegalArgumentException(
                    "GraphAr edge controls disagree with topology scan: "
                            + edgeCount
                            + " != "
                            + loadedEdges);
        }
        byte[] manifest = encodeManifest(vertexCount, edgeCount, (int) shardCount);
        if (cache.getAndPutIfAbsent(ShardKey.manifest(snapshotId), manifest) != null) {
            throw new IllegalStateException(
                    "CSR snapshot was concurrently published: " + snapshotId);
        }
        return new LoadResult(snapshotId, vertexCount, loadedEdges, (int) shardCount);
    }

    /** Executes a neighbor lookup colocated with the shard cache entry's primary owner. */
    public long[] neighbors(String snapshotId, long vertexId) {
        requireNonBlank(snapshotId, "Snapshot ID");
        if (vertexId < 0) {
            throw new IllegalArgumentException("Vertex ID must be non-negative.");
        }
        ensurePublished(snapshotId);
        ShardKey key = new ShardKey(snapshotId, shardIndex(vertexId));
        return ignite.compute()
                .affinityCall(cacheName, key, new NeighborCall(cacheName, key, vertexId));
    }

    /** Returns the node on which a colocated lookup for the requested vertex executes. */
    public UUID neighborLookupNodeId(String snapshotId, long vertexId) {
        requireNonBlank(snapshotId, "Snapshot ID");
        if (vertexId < 0) {
            throw new IllegalArgumentException("Vertex ID must be non-negative.");
        }
        ensurePublished(snapshotId);
        ShardKey key = new ShardKey(snapshotId, shardIndex(vertexId));
        return ignite.compute().affinityCall(cacheName, key, new LookupNodeCall(cacheName, key));
    }

    /**
     * Expands a frontier for {@code hops} rounds. Every round groups source vertices by their CSR
     * shard, so it makes at most one colocated compute call per populated shard. Bounds are
     * mandatory so high-degree graphs cannot create an unbounded result in the caller heap.
     */
    public TraversalResult traverse(
            String snapshotId, Collection<Long> seeds, int hops, int maxFrontierSize) {
        requireNonBlank(snapshotId, "Snapshot ID");
        if (hops < 0 || maxFrontierSize < 0) {
            throw new IllegalArgumentException(
                    "Hop count and frontier bound must be non-negative.");
        }
        Set<Long> frontier = new LinkedHashSet<>();
        for (Long seed : Objects.requireNonNull(seeds, "Seeds cannot be null.")) {
            if (seed == null || seed < 0) {
                throw new IllegalArgumentException("Seed vertex IDs must be non-negative.");
            }
            addBounded(frontier, seed, maxFrontierSize);
        }
        List<Integer> sizes = new ArrayList<>();
        List<Integer> shardCalls = new ArrayList<>();
        sizes.add(frontier.size());
        for (int hop = 0; hop < hops; hop++) {
            Set<Long> next = new LinkedHashSet<>();
            Map<ShardKey, List<Long>> sourcesByShard = new LinkedHashMap<>();
            for (long source : frontier) {
                sourcesByShard
                        .computeIfAbsent(
                                new ShardKey(snapshotId, shardIndex(source)),
                                unused -> new ArrayList<>())
                        .add(source);
            }
            shardCalls.add(sourcesByShard.size());
            for (Map.Entry<ShardKey, List<Long>> entry : sourcesByShard.entrySet()) {
                for (long destination :
                        ignite.compute()
                                .affinityCall(
                                        cacheName,
                                        entry.getKey(),
                                        new ExpandShardCall(
                                                cacheName,
                                                entry.getKey(),
                                                entry.getValue(),
                                                maxFrontierSize))) {
                    addBounded(next, destination, maxFrontierSize);
                }
            }
            frontier = next;
            sizes.add(frontier.size());
        }
        return new TraversalResult(
                snapshotId, List.copyOf(frontier), List.copyOf(sizes), List.copyOf(shardCalls));
    }

    /**
     * Returns whether the primary local node holds the specified shard in Ignite off-heap storage.
     */
    public boolean isLocalOffHeap(String snapshotId, int shardIndex) {
        ShardKey key = new ShardKey(snapshotId, shardIndex);
        return cache().localPeek(key, CachePeekMode.OFFHEAP) != null;
    }

    /** Returns the primary Ignite node selected by affinity for the immutable CSR shard. */
    public UUID primaryNodeId(String snapshotId, int shardIndex) {
        return ignite.affinity(cacheName).mapKeyToNode(new ShardKey(snapshotId, shardIndex)).id();
    }

    private int shardIndex(long vertexId) {
        long index = vertexId / verticesPerShard;
        if (index > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Vertex ID exceeds Ignite CSR shard index range.");
        }
        return (int) index;
    }

    private CsrBuilder newBuilder(long vertexCount, int shardIndex) {
        long vertexBase = Math.multiplyExact((long) shardIndex, verticesPerShard);
        return new CsrBuilder(
                vertexBase,
                (int) Math.min(verticesPerShard, Math.subtractExact(vertexCount, vertexBase)));
    }

    private static long publish(
            IgniteCache<ShardKey, byte[]> cache,
            String snapshotId,
            int shardIndex,
            CsrBuilder builder) {
        builder.finish();
        ShardKey key = new ShardKey(snapshotId, shardIndex);
        if (cache.getAndPutIfAbsent(key, builder.encode()) != null) {
            throw new IllegalStateException("CSR snapshot shard already exists: " + key);
        }
        return builder.edgeCount();
    }

    private void ensurePublished(String snapshotId) {
        if (cache().get(ShardKey.manifest(snapshotId)) == null) {
            throw new IllegalStateException("CSR snapshot is not published: " + snapshotId);
        }
    }

    @SuppressWarnings("unchecked")
    private IgniteCache<ShardKey, byte[]> cache() {
        return ignite.<ShardKey, byte[]>cache(cacheName);
    }

    private static byte[] encodeManifest(long vertexCount, long edgeCount, int shardCount) {
        return ByteBuffer.allocate(Long.BYTES * 2 + Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(vertexCount)
                .putLong(edgeCount)
                .putInt(shardCount)
                .array();
    }

    private static long divideRoundUp(long value, int divisor) {
        return value == 0 ? 0 : ((value - 1) / divisor) + 1;
    }

    private static void addBounded(Set<Long> values, long value, int maximum) {
        values.add(value);
        if (values.size() > maximum) {
            throw new IllegalArgumentException(
                    "Traversal frontier exceeds configured limit: " + maximum);
        }
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be blank.");
        }
        return value;
    }

    /** Immutable observable result of loading one GraphAr snapshot. */
    public static final class LoadResult {
        private final String snapshotId;
        private final long vertexCount;
        private final long edgeCount;
        private final int shardCount;

        private LoadResult(String snapshotId, long vertexCount, long edgeCount, int shardCount) {
            this.snapshotId = snapshotId;
            this.vertexCount = vertexCount;
            this.edgeCount = edgeCount;
            this.shardCount = shardCount;
        }

        public String snapshotId() {
            return snapshotId;
        }

        public long vertexCount() {
            return vertexCount;
        }

        public long edgeCount() {
            return edgeCount;
        }

        public int shardCount() {
            return shardCount;
        }
    }

    /** Immutable bounded multi-hop result. */
    public static final class TraversalResult {
        private final String snapshotId;
        private final List<Long> frontier;
        private final List<Integer> frontierSizes;
        private final List<Integer> shardCallsPerHop;

        private TraversalResult(
                String snapshotId,
                List<Long> frontier,
                List<Integer> frontierSizes,
                List<Integer> shardCallsPerHop) {
            this.snapshotId = snapshotId;
            this.frontier = frontier;
            this.frontierSizes = frontierSizes;
            this.shardCallsPerHop = shardCallsPerHop;
        }

        public String snapshotId() {
            return snapshotId;
        }

        public List<Long> frontier() {
            return frontier;
        }

        public List<Integer> frontierSizes() {
            return frontierSizes;
        }

        /** Returns the number of affinity jobs submitted in each traversal round. */
        public List<Integer> shardCallsPerHop() {
            return shardCallsPerHop;
        }
    }

    private static final class ShardKey implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String snapshotId;
        @AffinityKeyMapped private final int shardIndex;

        private ShardKey(String snapshotId, int shardIndex) {
            this.snapshotId = snapshotId;
            this.shardIndex = shardIndex;
        }

        private static ShardKey manifest(String snapshotId) {
            return new ShardKey(snapshotId, -1);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ShardKey)) {
                return false;
            }
            ShardKey key = (ShardKey) other;
            return shardIndex == key.shardIndex && snapshotId.equals(key.snapshotId);
        }

        @Override
        public int hashCode() {
            return 31 * snapshotId.hashCode() + shardIndex;
        }

        @Override
        public String toString() {
            return snapshotId + "/" + shardIndex;
        }
    }

    private static final class NeighborCall implements IgniteCallable<long[]> {
        private static final long serialVersionUID = 1L;
        private final String cacheName;
        private final ShardKey key;
        private final long vertexId;
        @IgniteInstanceResource private transient Ignite ignite;

        private NeighborCall(String cacheName, ShardKey key, long vertexId) {
            this.cacheName = cacheName;
            this.key = key;
            this.vertexId = vertexId;
        }

        @Override
        public long[] call() {
            byte[] bytes =
                    ignite.<ShardKey, byte[]>cache(cacheName).localPeek(key, CachePeekMode.PRIMARY);
            if (bytes == null) {
                throw new IllegalStateException(
                        "CSR shard is not available on its primary node: " + key);
            }
            return decodeNeighbors(bytes, vertexId);
        }
    }

    private static final class LookupNodeCall implements IgniteCallable<UUID> {
        private static final long serialVersionUID = 1L;
        private final String cacheName;
        private final ShardKey key;
        @IgniteInstanceResource private transient Ignite ignite;

        private LookupNodeCall(String cacheName, ShardKey key) {
            this.cacheName = cacheName;
            this.key = key;
        }

        @Override
        public UUID call() {
            if (ignite.<ShardKey, byte[]>cache(cacheName).localPeek(key, CachePeekMode.PRIMARY)
                    == null) {
                throw new IllegalStateException(
                        "CSR shard is not local to its primary node: " + key);
            }
            return ignite.cluster().localNode().id();
        }
    }

    private static final class ExpandShardCall implements IgniteCallable<long[]> {
        private static final long serialVersionUID = 1L;
        private final String cacheName;
        private final ShardKey key;
        private final List<Long> sources;
        private final int maxResults;
        @IgniteInstanceResource private transient Ignite ignite;

        private ExpandShardCall(
                String cacheName, ShardKey key, List<Long> sources, int maxResults) {
            this.cacheName = cacheName;
            this.key = key;
            this.sources = new ArrayList<>(sources);
            this.maxResults = maxResults;
        }

        @Override
        public long[] call() {
            byte[] bytes =
                    ignite.<ShardKey, byte[]>cache(cacheName).localPeek(key, CachePeekMode.PRIMARY);
            if (bytes == null) {
                throw new IllegalStateException(
                        "CSR shard is not available on its primary node: " + key);
            }
            Set<Long> destinations = new LinkedHashSet<>();
            for (long source : sources) {
                for (long destination : decodeNeighbors(bytes, source)) {
                    destinations.add(destination);
                    if (destinations.size() > maxResults) {
                        throw new IllegalArgumentException(
                                "CSR shard expansion exceeds configured frontier limit: "
                                        + maxResults);
                    }
                }
            }
            long[] result = new long[destinations.size()];
            int index = 0;
            for (long destination : destinations) {
                result[index++] = destination;
            }
            return result;
        }
    }

    private static final class CsrBuilder {
        private final long vertexBase;
        private final long[] offsets;
        private final List<Long> destinations = new ArrayList<>();
        private int nextOffset;
        private boolean finished;

        private CsrBuilder(long vertexBase, int vertexCount) {
            this.vertexBase = vertexBase;
            this.offsets = new long[vertexCount + 1];
            this.nextOffset = 1;
        }

        private void add(long source, long destination) {
            if (finished || source < vertexBase || source >= vertexBase + offsets.length - 1) {
                throw new IllegalArgumentException("Topology row does not fit its CSR shard.");
            }
            while (nextOffset <= source - vertexBase) {
                offsets[nextOffset++] = destinations.size();
            }
            destinations.add(destination);
        }

        private long vertexEnd() {
            return vertexBase + offsets.length - 1L;
        }

        private void finish() {
            if (!finished) {
                while (nextOffset < offsets.length) {
                    offsets[nextOffset++] = destinations.size();
                }
                finished = true;
            }
        }

        private long edgeCount() {
            return destinations.size();
        }

        private byte[] encode() {
            if (!finished) {
                throw new IllegalStateException("CSR shard must be finalized before encoding.");
            }
            int bytes =
                    Math.toIntExact(
                            Math.addExact(
                                    HEADER_BYTES,
                                    Math.addExact(
                                            Math.multiplyExact((long) offsets.length, Long.BYTES),
                                            Math.multiplyExact(
                                                    (long) destinations.size(), Long.BYTES))));
            ByteBuffer buffer = ByteBuffer.allocate(bytes).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(FORMAT_VERSION);
            buffer.putInt(offsets.length - 1);
            buffer.putInt(destinations.size());
            buffer.putLong(vertexBase);
            for (long offset : offsets) {
                buffer.putLong(offset);
            }
            for (long destination : destinations) {
                buffer.putLong(destination);
            }
            return buffer.array();
        }
    }

    private static long[] decodeNeighbors(byte[] bytes, long vertexId) {
        if (bytes == null || bytes.length < HEADER_BYTES) {
            throw new IllegalArgumentException("Invalid CSR shard payload.");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int format = buffer.getInt();
        int vertices = buffer.getInt();
        int edges = buffer.getInt();
        long vertexBase = buffer.getLong();
        if (format != FORMAT_VERSION
                || vertices < 0
                || edges < 0
                || vertexId < vertexBase
                || vertexId >= vertexBase + vertices) {
            throw new IllegalArgumentException("Invalid CSR shard or vertex ID.");
        }
        long expectedBytes =
                Math.addExact(
                        HEADER_BYTES,
                        Math.addExact(
                                Math.multiplyExact((long) vertices + 1, Long.BYTES),
                                Math.multiplyExact((long) edges, Long.BYTES)));
        if (expectedBytes != bytes.length) {
            throw new IllegalArgumentException("Invalid CSR shard payload size.");
        }
        int local = Math.toIntExact(vertexId - vertexBase);
        int offsetsStart = HEADER_BYTES;
        long begin = buffer.getLong(offsetsStart + local * Long.BYTES);
        long end = buffer.getLong(offsetsStart + (local + 1) * Long.BYTES);
        if (begin < 0 || end < begin || end > edges) {
            throw new IllegalArgumentException("Invalid CSR offsets in shard.");
        }
        long[] result = new long[Math.toIntExact(end - begin)];
        int destinationStart = offsetsStart + (vertices + 1) * Long.BYTES;
        for (int index = 0; index < result.length; index++) {
            result[index] =
                    buffer.getLong(destinationStart + Math.toIntExact(begin + index) * Long.BYTES);
        }
        return result;
    }
}
