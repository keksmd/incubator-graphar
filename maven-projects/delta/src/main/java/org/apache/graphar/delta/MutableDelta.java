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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.graphar.reader.CsrDirection;
import org.apache.graphar.reader.HeterogeneousCsr;
import org.apache.graphar.reader.ProjectionCapacity;
import org.apache.graphar.reader.VertexIdIndex;

/**
 * The mutable half of a graph whose other half is an immutable GraphAr projection.
 *
 * <p>A projection is rebuilt or extended in whole publications, which is the right shape for data
 * that lands at the end of the physical ordering and the wrong shape for data that patches the
 * adjacency of a vertex the projection already materialized. Turning every such patch into a small
 * immutable chunk buys freshness with the small-file problem and with read amplification that grows
 * without a bound. This class takes those patches instead, at the cost of one entry per adjacency
 * side, and hands them to readers as an overlay on the projection: {@code GraphView = Base +
 * Delta}.
 *
 * <p>Appends are serialized and reads take no lock. An edge is appended by writing it into an arena
 * and linking one entry per adjacency side in front of the chain of its vertex, which is O(1) and
 * never moves an entry that a reader may be walking. What a reader is allowed to see is decided by
 * sequence alone, so {@link #seal()} publishes a new view by moving one number rather than by
 * copying anything.
 *
 * <p>Vertices the base does not hold are named here as well, above its vertex count and in arrival
 * order. That ordering is a contract with compaction: a base that appends those vertices in the
 * same order keeps every global vertex number it had, which is what lets {@link #rebase} drop the
 * absorbed prefix without renumbering anything.
 *
 * <p>The delta is bounded on purpose. Reaching {@link DeltaOptions#maxEdges()} throws {@link
 * DeltaCapacityExceededException} rather than accepting a read penalty that nothing is watching,
 * because an unbounded delta is a slow base with extra steps.
 */
public final class MutableDelta {
    /** Returned when neither the base nor the delta names an identifier. */
    public static final long ABSENT = -1L;

    private final DeltaOptions options;
    private final DeltaJournal journal;
    private final CsrDirection direction;
    private final int chunkShift;
    private final int chunkMask;

    private HeterogeneousCsr base;
    private String[] types;
    private long baseVertexCount;
    private GrowableIdIndex[] overlayByType;
    private String[] overlayIdentifiers;
    private int[] overlayTypes;
    private int overlayCount;
    private VertexHeads heads;
    private int[][] edgeSources;
    private int[][] edgeTargets;
    private int[][] entryEdge;
    private int[][] entryLink;
    private long baseSequence;
    private long sequence;
    private int entryCount;
    private volatile GraphView published;

    private MutableDelta(
            HeterogeneousCsr base, DeltaOptions options, DeltaJournal journal, long baseSequence) {
        this.options = options;
        this.journal = journal;
        this.direction = base.direction();
        this.chunkShift = Integer.numberOfTrailingZeros(options.chunkSize());
        this.chunkMask = options.chunkSize() - 1;
        adopt(base, baseSequence);
    }

    /** Opens an empty delta on {@code base} that keeps no durable record of what it is told. */
    public static MutableDelta on(HeterogeneousCsr base) {
        return on(base, DeltaOptions.defaults(), DeltaJournal.none());
    }

    /** Opens an empty delta on {@code base}. */
    public static MutableDelta on(
            HeterogeneousCsr base, DeltaOptions options, DeltaJournal journal) {
        Objects.requireNonNull(base, "Base projection cannot be null.");
        Objects.requireNonNull(options, "Delta options cannot be null.");
        Objects.requireNonNull(journal, "Delta journal cannot be null.");
        return new MutableDelta(base, options, journal, journal.edgeFloor());
    }

    /**
     * Opens a delta on {@code base} holding everything {@code journal} recorded.
     *
     * <p>Vertex numbers come out of the replay identical to the ones handed out before the restart,
     * because they are positions in the order the journal preserves. A journal that belongs to a
     * different base refuses to open rather than replaying into the wrong vertex space.
     */
    public static MutableDelta recover(
            HeterogeneousCsr base, DeltaOptions options, DeltaJournal journal) throws IOException {
        MutableDelta delta = on(base, options, journal);
        journal.replay(
                new DeltaJournal.Visitor() {
                    @Override
                    public void vertex(int typeOrdinal, String externalId) {
                        delta.allocate(typeOrdinal, externalId, false);
                    }

                    @Override
                    public void edge(int source, int target) {
                        delta.record(source, target, false);
                    }
                });
        delta.seal();
        return delta;
    }

    /** Returns the projection this delta extends. */
    public HeterogeneousCsr base() {
        return base;
    }

    /** Returns the view published by the last {@link #seal()}. */
    public GraphView current() {
        return published;
    }

    /**
     * Publishes everything appended so far as one view and returns it.
     *
     * <p>The cadence of this call is the freshness of the graph: nothing appended after it is
     * visible, and everything appended before it is. It copies nothing, so a caller that wants
     * seconds calls it every second.
     */
    public synchronized GraphView seal() {
        published =
                GraphView.of(
                        base,
                        new DeltaSnapshot(
                                direction,
                                types,
                                baseVertexCount,
                                overlayCount,
                                overlayIdentifiers,
                                overlayTypes,
                                overlayByType.clone(),
                                heads,
                                entryEdge,
                                entryLink,
                                edgeSources,
                                edgeTargets,
                                chunkShift,
                                chunkMask,
                                baseSequence,
                                sequence));
        return published;
    }

    /** Makes every append made so far durable. */
    public synchronized void sync() throws IOException {
        journal.sync();
    }

    /**
     * Resolves an application identifier to a global vertex number, naming it in the delta when the
     * base does not hold it.
     */
    public synchronized long vertexId(String vertexType, String externalId) {
        Objects.requireNonNull(externalId, "External identifier cannot be null.");
        long known = lookup(vertexType, externalId);
        if (known != ABSENT) {
            return known;
        }
        return allocate(typeOrdinal(vertexType), externalId, true);
    }

    /** Resolves an application identifier without naming it, or returns {@link #ABSENT}. */
    public synchronized long lookup(String vertexType, String externalId) {
        Objects.requireNonNull(externalId, "External identifier cannot be null.");
        int ordinal = typeOrdinal(vertexType);
        long inBase = base.globalIndex(types[ordinal], externalId);
        if (inBase != VertexIdIndex.ABSENT) {
            return inBase;
        }
        int inOverlay = overlayByType[ordinal].lookup(externalId);
        return inOverlay == GrowableIdIndex.ABSENT ? ABSENT : inOverlay;
    }

    /** Appends one edge between two global vertex numbers and returns the sequence it was given. */
    public synchronized long append(long source, long target) throws IOException {
        long assigned = sequence;
        record(checked(source), checked(target), true);
        return assigned;
    }

    /** Appends {@code count} edges and returns the sequence the next append would be given. */
    public synchronized long appendAll(long[] sources, long[] targets, int count)
            throws IOException {
        Objects.requireNonNull(sources, "Appended sources cannot be null.");
        Objects.requireNonNull(targets, "Appended targets cannot be null.");
        if (count < 0 || count > sources.length || count > targets.length) {
            throw new IllegalArgumentException(
                    "Appended edge count is outside the endpoints supplied: " + count);
        }
        for (int edge = 0; edge < count; edge++) {
            record(checked(sources[edge]), checked(targets[edge]), true);
        }
        return sequence;
    }

    /** Returns the sequence the next appended edge will be given. */
    public synchronized long sequence() {
        return sequence;
    }

    /** Returns the sequence up to which the base already holds the edges. */
    public synchronized long baseSequence() {
        return baseSequence;
    }

    /** Returns the number of edges the delta holds. */
    public synchronized long edgeCount() {
        return sequence - baseSequence;
    }

    /** Returns the number of vertices the delta named that its base does not hold. */
    public synchronized int overlayCount() {
        return overlayCount;
    }

    /**
     * Moves the delta onto a base that absorbed its edges below {@code includedSequence}, dropping
     * exactly what the new base now holds.
     *
     * <p>This is the second half of a compaction: the first half writes the affected chunks and
     * publishes the projection, this one stops readers from paying for those edges twice. The new
     * base may also have absorbed a prefix of the vertices the delta named, which its vertex count
     * declares; those vertices keep the numbers they were given, so nothing in the surviving delta
     * is renumbered.
     *
     * <p>Views published earlier stay valid and keep answering from what they were taken over. They
     * see the base and the delta they were paired with, and neither is mutated here.
     */
    public synchronized GraphView rebase(HeterogeneousCsr rebased, long includedSequence)
            throws IOException {
        Objects.requireNonNull(rebased, "Rebased projection cannot be null.");
        if (rebased.direction() != direction) {
            throw new IllegalArgumentException(
                    "Rebased projection is "
                            + rebased.direction()
                            + " where the delta is "
                            + direction
                            + ".");
        }
        if (includedSequence < baseSequence || includedSequence > sequence) {
            throw new IllegalArgumentException(
                    "Absorbed sequence is outside the delta ["
                            + baseSequence
                            + ", "
                            + sequence
                            + "]: "
                            + includedSequence);
        }
        long absorbedVertices = rebased.vertexCount() - baseVertexCount;
        if (absorbedVertices < 0 || absorbedVertices > overlayCount) {
            throw new IllegalArgumentException(
                    "Rebased projection holds "
                            + rebased.vertexCount()
                            + " vertices, which is not the base of "
                            + baseVertexCount
                            + " plus a prefix of the "
                            + overlayCount
                            + " the delta named.");
        }
        int absorbed = (int) absorbedVertices;
        String[] survivingIdentifiers =
                Arrays.copyOfRange(overlayIdentifiers, absorbed, overlayCount);
        int[] survivingTypes = Arrays.copyOfRange(overlayTypes, absorbed, overlayCount);
        int[][] previousSources = edgeSources;
        int[][] previousTargets = edgeTargets;
        long previousFloor = baseSequence;
        long previousSequence = sequence;

        adopt(rebased, includedSequence);
        for (int ordinal = 0; ordinal < survivingIdentifiers.length; ordinal++) {
            allocate(survivingTypes[ordinal], survivingIdentifiers[ordinal], false);
        }
        for (long moved = includedSequence; moved < previousSequence; moved++) {
            int offset = (int) (moved - previousFloor);
            record(
                    previousSources[offset >>> chunkShift][offset & chunkMask],
                    previousTargets[offset >>> chunkShift][offset & chunkMask],
                    false);
        }
        DeltaSnapshot rewritten = seal().delta();
        journal.rewrite(
                baseVertexCount,
                includedSequence,
                target -> {
                    for (int ordinal = 0; ordinal < rewritten.overlayCount(); ordinal++) {
                        long vertex = baseVertexCount + ordinal;
                        target.vertex(
                                typeOrdinal(rewritten.overlayType(vertex)),
                                rewritten.overlayIdentifier(vertex));
                    }
                    for (long moved = includedSequence; moved < rewritten.sequence(); moved++) {
                        target.edge(
                                (int) rewritten.edgeSource(moved),
                                (int) rewritten.edgeTarget(moved));
                    }
                });
        return published;
    }

    private void adopt(HeterogeneousCsr adopted, long floor) {
        this.base = adopted;
        List<String> declared = adopted.vertexTypes();
        this.types = declared.toArray(new String[0]);
        this.baseVertexCount = adopted.vertexCount();
        this.overlayByType = new GrowableIdIndex[types.length];
        for (int ordinal = 0; ordinal < types.length; ordinal++) {
            overlayByType[ordinal] = new GrowableIdIndex();
        }
        this.overlayIdentifiers = new String[16];
        this.overlayTypes = new int[16];
        this.overlayCount = 0;
        this.heads = new VertexHeads(options.initialVertexCapacity());
        this.edgeSources = new int[chunkCount(options.maxEdges())][];
        this.edgeTargets = new int[chunkCount(options.maxEdges())][];
        this.entryEdge = new int[chunkCount(maxEntries())][];
        this.entryLink = new int[chunkCount(maxEntries())][];
        this.baseSequence = floor;
        this.sequence = floor;
        this.entryCount = 0;
        seal();
    }

    private long allocate(int typeOrdinal, String externalId, boolean record) {
        long vertex = baseVertexCount + overlayCount;
        if (vertex > ProjectionCapacity.MAX_VERTICES) {
            throw new IllegalStateException(
                    "The base and the delta together name more vertices than a projection"
                            + " addresses: "
                            + vertex);
        }
        int assigned = overlayByType[typeOrdinal].putIfAbsent(externalId, (int) vertex);
        if (assigned != (int) vertex) {
            return assigned;
        }
        if (overlayCount == overlayIdentifiers.length) {
            overlayIdentifiers = Arrays.copyOf(overlayIdentifiers, overlayCount * 2);
            overlayTypes = Arrays.copyOf(overlayTypes, overlayCount * 2);
        }
        overlayIdentifiers[overlayCount] = externalId;
        overlayTypes[overlayCount] = typeOrdinal;
        overlayCount++;
        if (overlayByType[typeOrdinal].isFull()) {
            overlayByType[typeOrdinal] = overlayByType[typeOrdinal].grown();
        }
        if (record) {
            try {
                journal.vertex(typeOrdinal, externalId);
            } catch (IOException failure) {
                throw new UncheckedDeltaException(
                        "Failed to record vertex " + externalId + " of type " + typeOrdinal,
                        failure);
            }
        }
        return vertex;
    }

    private void record(int source, int target, boolean record) {
        if (sequence - baseSequence >= options.maxEdges()) {
            throw new DeltaCapacityExceededException(sequence - baseSequence, options.maxEdges());
        }
        int offset = (int) (sequence - baseSequence);
        write(edgeSources, offset, source);
        write(edgeTargets, offset, target);
        sequence++;
        if (direction != CsrDirection.INCOMING) {
            link(source, offset);
        }
        if (direction != CsrDirection.OUTGOING) {
            link(target, offset);
        }
        if (record) {
            try {
                journal.edge(source, target);
            } catch (IOException failure) {
                throw new UncheckedDeltaException(
                        "Failed to record edge " + source + " -> " + target, failure);
            }
        }
    }

    private void link(int vertex, int edgeOffset) {
        int entry = entryCount++;
        write(entryEdge, entry, edgeOffset);
        write(entryLink, entry, heads.head(vertex));
        if (heads.isFull()) {
            heads = heads.grown();
        }
        heads.setHead(vertex, entry);
    }

    private void write(int[][] chunks, int offset, int value) {
        int chunk = offset >>> chunkShift;
        if (chunks[chunk] == null) {
            chunks[chunk] = new int[options.chunkSize()];
        }
        chunks[chunk][offset & chunkMask] = value;
    }

    private int checked(long vertex) {
        if (vertex < 0 || vertex >= baseVertexCount + overlayCount) {
            throw new IllegalArgumentException("Vertex is outside the view: " + vertex);
        }
        return (int) vertex;
    }

    private int typeOrdinal(String vertexType) {
        Objects.requireNonNull(vertexType, "Vertex type cannot be null.");
        for (int ordinal = 0; ordinal < types.length; ordinal++) {
            if (types[ordinal].equals(vertexType)) {
                return ordinal;
            }
        }
        throw new IllegalArgumentException("Undeclared vertex type: " + vertexType);
    }

    private long maxEntries() {
        return direction == CsrDirection.UNDIRECTED ? options.maxEdges() * 2L : options.maxEdges();
    }

    private int chunkCount(long slots) {
        return Math.toIntExact((slots + options.chunkSize() - 1) / options.chunkSize());
    }
}
