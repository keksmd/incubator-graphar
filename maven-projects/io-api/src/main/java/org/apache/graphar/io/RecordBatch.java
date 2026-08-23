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

/** A format-neutral, finite set of same-length vectors sharing one schema. */
public interface RecordBatch {
    /** Returns the schema that defines the vectors in physical column order. */
    Schema schema();

    /** Returns the number of rows in this batch. */
    int rowCount();

    /** Returns the number of vectors in this batch. */
    int columnCount();

    /** Returns the vector at zero-based physical {@code index}. */
    ValueVector column(int index);
}
