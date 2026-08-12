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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

/** Optional warmed full-topology scan benchmark matching C++ and Spark runtime harnesses. */
public class TopologyScanBenchmarkTest {
    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASURED_ITERATIONS = 10;

    @Test
    public void scansCanonicalTopology() throws Exception {
        String configured = System.getProperty("graphar.bench.graph");
        if (configured == null || configured.isBlank()) {
            return;
        }
        Path graphPath = Path.of(configured).toAbsolutePath();
        GraphReader graph =
                GraphReader.open(
                        graphPath.toUri(),
                        new LocalFileSystemStringGraphInfoLoader(),
                        new LocalStorage(),
                        new ParquetPhysicalReader(new LocalStorage()));
        OrderedSourceEdgeReader edges = graph.edge("person", "knows", "person");
        for (int iteration = 0; iteration < WARMUP_ITERATIONS; iteration++) {
            assertEquals(6626L, scan(edges));
        }
        long started = System.nanoTime();
        long checksum = 0;
        for (int iteration = 0; iteration < MEASURED_ITERATIONS; iteration++) {
            checksum += scan(edges);
        }
        long elapsedNanos = System.nanoTime() - started;
        assertEquals(66260L, checksum);
        System.out.println(
                "JAVA_TOPOLOGY_SCAN rows=6626 warmup="
                        + WARMUP_ITERATIONS
                        + " iterations="
                        + MEASURED_ITERATIONS
                        + " total_ms="
                        + elapsedNanos / 1_000_000.0
                        + " avg_ms="
                        + elapsedNanos / 1_000_000.0 / MEASURED_ITERATIONS);
    }

    private static long scan(OrderedSourceEdgeReader edges) throws Exception {
        long rows = 0;
        long checksum = 0;
        try (EdgeCursor cursor = edges.scanEdges()) {
            while (cursor.next()) {
                // Touch both values so the benchmark is a topology decode, not just cursor
                // movement.
                checksum += cursor.source() * 31 + cursor.destination();
                rows++;
            }
        }
        assertTrue(checksum != 0);
        return rows;
    }
}
