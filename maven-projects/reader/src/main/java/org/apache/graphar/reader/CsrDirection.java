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

/**
 * Selects which adjacency a materialized CSR exposes for each vertex.
 *
 * <p>A GraphAr {@code ordered_by_source} topology stores outgoing adjacency only. Serving an
 * undirected neighbourhood from it therefore requires either a second {@code ordered_by_dest}
 * projection on disk or a transposition while the CSR is being built. The latter reads the topology
 * once and is what {@link #INCOMING} and {@link #UNDIRECTED} do.
 *
 * <p>{@link #INCOMING} and {@link #UNDIRECTED} reverse edges, so they are defined only when the
 * source and destination vertex types of the edge are the same type. Reversing across two vertex
 * types would index destinations in the wrong identifier space.
 */
public enum CsrDirection {
    /** Adjacency of a vertex is the destinations of its outgoing edges. */
    OUTGOING,
    /** Adjacency of a vertex is the sources of its incoming edges. */
    INCOMING,
    /**
     * Adjacency of a vertex is both. Every edge contributes one entry in each direction, so a
     * self-loop appears twice and a repeated edge keeps its multiplicity.
     */
    UNDIRECTED
}
