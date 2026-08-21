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
 * Raised when the durable record of an append fails after the append was already applied in memory.
 *
 * <p>The in-memory state and the journal are written in that order, so a failure here leaves a
 * delta holding an edge that a restart would not recover. Callers that need the two to agree stop
 * using the delta and rebuild it from the journal; callers that can replay from their own ledger
 * may keep going. Either way the failure is not the caller's ordinary control flow, which is why it
 * does not turn every append into a checked-exception site.
 */
public class UncheckedDeltaException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /** Creates an exception describing the append whose durable record failed. */
    public UncheckedDeltaException(String message, IOException cause) {
        super(message, cause);
    }
}
