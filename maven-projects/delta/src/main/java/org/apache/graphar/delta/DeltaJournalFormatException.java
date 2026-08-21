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

/**
 * Raised when a journal file is not one this delta may replay.
 *
 * <p>A journal is only meaningful next to the base it was written against, because the vertex
 * numbers it records are positions in that base's vertex space. Opening it against a different
 * base, or against a file that is not a journal at all, is refused here rather than discovered
 * later as an adjacency attached to the wrong vertex. A torn tail is not this: an append that the
 * process did not survive is truncated on open, which is the normal end of an unclean shutdown.
 */
public class DeltaJournalFormatException extends IOException {
    private static final long serialVersionUID = 1L;

    /** Creates an exception describing why the journal cannot be replayed. */
    public DeltaJournalFormatException(String message) {
        super(message);
    }
}
