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

/**
 * One typed, columnar sequence in a {@link RecordBatch}.
 *
 * <p>This deliberately follows the read shape of an Apache Arrow value vector without making the
 * dependency-light IO API own Arrow buffers or allocators. A format adapter may expose its native
 * vector directly when its lifetime permits, or adapt it through this interface.
 */
public interface ValueVector {
    /** Returns the field that describes values in this vector. */
    Field field();

    /** Returns the number of values in this vector. */
    int valueCount();

    /** Returns whether the value at zero-based {@code index} is null. */
    boolean isNull(int index);

    /**
     * Returns the value at zero-based {@code index}, or {@code null} when {@link #isNull(int)} is
     * true. The Java representation is defined by the vector implementation and its {@link Field}.
     */
    Object getObject(int index);
}
