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

package org.apache.graphar.io;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/** One row in a {@link RecordBatch}, accessed by zero-based schema column index. */
public interface Row {
    /**
     * Returns the value for {@code columnIndex}, or {@code null} only for a nullable field. Values
     * are represented as follows: BOOLEAN by {@link Boolean}; INT8, INT16, INT32, and INT64 by the
     * corresponding boxed number; FLOAT32 and FLOAT64 by {@link Float} and {@link Double}; STRING
     * by {@link String}; BINARY by a read-only {@link ByteBuffer}; DATE by {@link LocalDate};
     * TIMESTAMP_MILLIS by {@link Instant}; and LIST by an unmodifiable {@link List} whose elements
     * recursively follow the element type mapping.
     */
    Object value(int columnIndex);
}
