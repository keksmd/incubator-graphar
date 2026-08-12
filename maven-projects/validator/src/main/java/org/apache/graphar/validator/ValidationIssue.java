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

package org.apache.graphar.validator;

import java.net.URI;
import java.util.Objects;

/** One machine-readable dataset validation finding. */
public final class ValidationIssue {
    /** Severity of a finding. */
    public enum Severity {
        ERROR,
        WARNING
    }

    private final Severity severity;
    private final String code;
    private final URI location;
    private final String message;

    ValidationIssue(Severity severity, String code, URI location, String message) {
        this.severity = Objects.requireNonNull(severity, "Severity cannot be null.");
        this.code = Objects.requireNonNull(code, "Code cannot be null.");
        this.location = location;
        this.message = Objects.requireNonNull(message, "Message cannot be null.");
    }

    public Severity severity() {
        return severity;
    }

    public String code() {
        return code;
    }

    public URI location() {
        return location;
    }

    public String message() {
        return message;
    }

    @Override
    public String toString() {
        return severity + " " + code + " " + location + ": " + message;
    }
}
