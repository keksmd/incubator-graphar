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

/** Bounded-memory tuning for {@link GraphWriter#writeEdgeLayout}. */
public final class EdgeWriteOptions {
    private static final int DEFAULT_MAX_RECORDS_IN_MEMORY = 65_536;

    private final int maxRecordsInMemory;

    /** Creates an option set with the maximum number of records held by one sort run. */
    public EdgeWriteOptions(int maxRecordsInMemory) {
        if (maxRecordsInMemory <= 0) {
            throw new IllegalArgumentException("Maximum records in memory must be positive.");
        }
        this.maxRecordsInMemory = maxRecordsInMemory;
    }

    /** Returns the production default, which bounds each sort run to 65,536 records. */
    public static EdgeWriteOptions defaults() {
        return new EdgeWriteOptions(DEFAULT_MAX_RECORDS_IN_MEMORY);
    }

    /** Returns the maximum number of edge records buffered while creating a sorted run. */
    public int maxRecordsInMemory() {
        return maxRecordsInMemory;
    }
}
