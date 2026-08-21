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

import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.apache.graphar.info.AdjacentList;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.info.type.AdjListType;
import org.apache.graphar.info.type.DataType;
import org.apache.graphar.info.type.FileType;

/** Builds in-memory edge metadata so core tests need no stored graph. */
final class EdgeInfoFixtures {
    private EdgeInfoFixtures() {}

    /** Returns metadata declaring both ordered layouts with distinct endpoint chunk sizes. */
    static EdgeInfo personKnowsPerson(long edgeChunkSize, long srcChunkSize, long dstChunkSize) {
        return EdgeInfo.builder()
                .edgeTriplet("person", "knows", "person")
                .chunkSize(edgeChunkSize)
                .srcChunkSize(srcChunkSize)
                .dstChunkSize(dstChunkSize)
                .directed(false)
                .baseUri(URI.create("edge/person_knows_person/"))
                .addAdjacentList(
                        new AdjacentList(
                                AdjListType.ordered_by_source,
                                FileType.PARQUET,
                                "ordered_by_source/"))
                .addAdjacentList(
                        new AdjacentList(
                                AdjListType.ordered_by_dest, FileType.PARQUET, "ordered_by_dest/"))
                .addPropertyGroup(
                        new PropertyGroup(
                                Collections.singletonList(
                                        new Property("creationDate", DataType.INT64, false, true)),
                                FileType.PARQUET,
                                "creationDate/"))
                .build();
    }

    /** Returns metadata that declares only the source-ordered layout. */
    static EdgeInfo sourceOrderedOnly(long edgeChunkSize, long srcChunkSize) {
        return EdgeInfo.builder()
                .edgeTriplet("person", "knows", "person")
                .chunkSize(edgeChunkSize)
                .srcChunkSize(srcChunkSize)
                .dstChunkSize(srcChunkSize)
                .directed(false)
                .baseUri(URI.create("edge/person_knows_person/"))
                .adjacentLists(
                        List.of(
                                new AdjacentList(
                                        AdjListType.ordered_by_source,
                                        FileType.PARQUET,
                                        "ordered_by_source/")))
                .addPropertyGroup(
                        new PropertyGroup(
                                Collections.singletonList(
                                        new Property("creationDate", DataType.INT64, false, true)),
                                FileType.PARQUET,
                                "creationDate/"))
                .build();
    }
}
