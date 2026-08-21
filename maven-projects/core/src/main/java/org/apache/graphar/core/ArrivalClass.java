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

package org.apache.graphar.core;

/**
 * The route an arriving edge takes, decided against a projection's physical ordering.
 *
 * <p>The split is a property of the chosen ordering, not of the event: the same edge classifies
 * differently under a different {@link AdjacencyOrdering}.
 */
public enum ArrivalClass {
    /**
     * The edge sorts at or above the frontier, so writing it appends rows past everything already
     * materialized and moves no existing row.
     */
    APPEND_TAIL,

    /**
     * The edge sorts below the frontier, so writing it inserts into the adjacency of an already
     * materialized vertex and shifts every row ordered after it.
     */
    PATCH
}
