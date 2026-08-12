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
import java.net.URI;
import java.util.Objects;
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.loader.GraphInfoLoader;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.storage.Storage;

/** Graph-facing entry point for the first pure-Java ordered-topology read path. */
public final class GraphReader {
    private final GraphInfo graphInfo;
    private final URI datasetRoot;
    private final Storage storage;
    private final PhysicalReader physicalReader;

    /** Opens a graph whose metadata has already been loaded by {@code graphar-info}. */
    public GraphReader(
            GraphInfo graphInfo, URI datasetRoot, Storage storage, PhysicalReader physicalReader) {
        this.graphInfo = Objects.requireNonNull(graphInfo, "Graph info cannot be null.");
        this.datasetRoot = DatasetUris.directory(datasetRoot);
        this.storage = Objects.requireNonNull(storage, "Storage cannot be null.");
        this.physicalReader =
                Objects.requireNonNull(physicalReader, "Physical reader cannot be null.");
    }

    /** Loads GraphAr metadata and opens a graph rooted at its declared base URI. */
    public static GraphReader open(
            URI graphYamlUri,
            GraphInfoLoader graphInfoLoader,
            Storage storage,
            PhysicalReader physicalReader)
            throws IOException {
        Objects.requireNonNull(graphYamlUri, "Graph YAML URI cannot be null.");
        GraphInfo loaded =
                Objects.requireNonNull(graphInfoLoader, "Graph info loader cannot be null.")
                        .loadGraphInfo(graphYamlUri);
        return new GraphReader(loaded, loaded.getBaseUri(), storage, physicalReader);
    }

    /** Returns the immutable metadata that defines this graph. */
    public GraphInfo graphInfo() {
        return graphInfo;
    }

    /** Opens an ordered-by-source reader for the supplied GraphAr edge triplet. */
    public OrderedSourceEdgeReader edge(String srcType, String edgeType, String dstType) {
        return new OrderedSourceEdgeReader(
                graphInfo.getEdgeInfo(srcType, edgeType, dstType),
                datasetRoot,
                storage,
                physicalReader);
    }

    /** Opens any declared GraphAr adjacency layout with topology and edge-property joins. */
    public EdgeLayoutReader edge(
            String srcType, String edgeType, String dstType, AdjListType layout) {
        return new EdgeLayoutReader(
                graphInfo.getEdgeInfo(srcType, edgeType, dstType),
                layout,
                datasetRoot,
                storage,
                physicalReader);
    }
}
