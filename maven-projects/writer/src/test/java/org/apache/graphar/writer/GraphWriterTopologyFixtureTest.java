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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.ReadCapability;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.io.parquet.ParquetPhysicalWriter;
import org.apache.graphar.reader.NeighborCursor;
import org.apache.graphar.reader.OrderedSourceNeighborReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

public class GraphWriterTopologyFixtureTest {
    @Test
    public void writesIndexedAdjacencyChunksForPartialNeighborReads() throws Exception {
        Path root = Files.createTempDirectory("graphar-indexed-topology-");
        LocalStorage storage = new LocalStorage();
        GraphInfo graphInfo =
                new LocalFileSystemStringGraphInfoLoader()
                        .loadGraphInfo(fixturePath().resolve("ldbc_sample.graph.yml").toUri());
        EdgeInfo edgeInfo = graphInfo.getEdgeInfo("person", "knows", "person");
        List<TopologyEdge> edges = new ArrayList<>();
        for (long destination = 10000; destination < 11300; destination++) {
            edges.add(new TopologyEdge(2, destination));
        }
        try {
            GraphWriter writer =
                    new GraphWriter(
                            graphInfo, root.toUri(), storage, new ParquetPhysicalWriter(storage));
            assertEquals(1300L, writer.writeOrderedSourceTopology(edgeInfo, 4, edges));

            List<Long> destinations = new ArrayList<>();
            List<org.apache.graphar.io.ReadReport> reports;
            try (NeighborCursor cursor =
                    new OrderedSourceNeighborReader(
                                    edgeInfo, root.toUri(), new ParquetPhysicalReader(storage))
                            .neighbors(2)) {
                while (cursor.next()) {
                    destinations.add(cursor.destination());
                }
                reports = cursor.reports();
            }

            assertEquals(1300, destinations.size());
            assertEquals(10000L, destinations.get(0).longValue());
            assertEquals(11299L, destinations.get(destinations.size() - 1).longValue());
            assertEquals(3, reports.size());
            for (int index = 1; index < reports.size(); index++) {
                assertTrue(reports.get(index).applied().contains(ReadCapability.ROW_RANGE));
                assertTrue(reports.get(index).declined().isEmpty());
            }
        } finally {
            deleteTree(root);
        }
    }

    private static Path fixturePath() {
        return Path.of("..", "..", "testing", "ldbc_sample", "parquet");
    }

    private static void deleteTree(Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            List<Path> files =
                    paths.sorted(java.util.Comparator.reverseOrder())
                            .collect(java.util.stream.Collectors.toList());
            for (Path path : files) {
                Files.delete(path);
            }
        }
    }
}
