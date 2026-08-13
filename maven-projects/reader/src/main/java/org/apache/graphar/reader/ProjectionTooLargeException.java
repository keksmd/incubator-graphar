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

/**
 * Raised when a graph is past what a heap CSR can hold, either because it cannot be addressed by an
 * array index or because the build would not fit in the heap.
 *
 * <p>It is a distinct type so a serving process can tell this apart from a malformed dataset: the
 * dataset is fine, the projection strategy is the wrong size for it.
 */
public final class ProjectionTooLargeException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    /** Creates an exception describing the graph and the limit it passed. */
    public ProjectionTooLargeException(String message) {
        super(message);
    }
}
