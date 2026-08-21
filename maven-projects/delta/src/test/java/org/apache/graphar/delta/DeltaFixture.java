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
import java.nio.file.Path;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.reader.CsrDirection;
import org.apache.graphar.reader.GraphReader;
import org.apache.graphar.reader.HeterogeneousCsr;
import org.apache.graphar.storage.local.LocalStorage;

/** Builds the canonical fixture projection the delta tests patch. */
final class DeltaFixture {
    static final String PERSON = "person";
    static final String PERSON_ID = "13194139533574";

    private DeltaFixture() {}

    /** Returns the LDBC sample projection, undirected as the reader fixtures build it. */
    static HeterogeneousCsr projection() throws IOException {
        return projection(CsrDirection.UNDIRECTED);
    }

    /** Returns the LDBC sample projection materialized in {@code direction}. */
    static HeterogeneousCsr projection(CsrDirection direction) throws IOException {
        return HeterogeneousCsr.builder(
                        GraphReader.open(
                                Path.of("..", "..", "testing", "ldbc_sample", "parquet")
                                        .resolve("ldbc_sample.graph.yml")
                                        .toUri(),
                                new LocalFileSystemStringGraphInfoLoader(),
                                new LocalStorage(),
                                new ParquetPhysicalReader(new LocalStorage())))
                .addVertexType(PERSON)
                .addEdgeType(PERSON, "knows", PERSON)
                .direction(direction)
                .build();
    }

    /** Returns a vertex the projection does not already put next to {@code vertex}. */
    static long unrelated(HeterogeneousCsr projection, long vertex) {
        for (long candidate = 0; candidate < projection.vertexCount(); candidate++) {
            if (candidate == vertex) {
                continue;
            }
            boolean adjacent = false;
            for (long neighbor : projection.neighbors(vertex)) {
                if (neighbor == candidate) {
                    adjacent = true;
                    break;
                }
            }
            if (!adjacent) {
                return candidate;
            }
        }
        throw new AssertionError("Every vertex is already adjacent to " + vertex);
    }
}
