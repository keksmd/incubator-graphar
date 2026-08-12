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
import java.util.ArrayList;
import java.util.List;

/** Immutable outcome of one format-neutral GraphAr dataset validation run. */
public final class ValidationReport {
    private final List<ValidationIssue> issues;
    private final long filesChecked;
    private final long rowsChecked;

    ValidationReport(List<ValidationIssue> issues, long filesChecked, long rowsChecked) {
        this.issues = List.copyOf(issues);
        this.filesChecked = filesChecked;
        this.rowsChecked = rowsChecked;
    }

    public boolean isValid() {
        return issues.stream()
                .noneMatch(issue -> issue.severity() == ValidationIssue.Severity.ERROR);
    }

    public List<ValidationIssue> issues() {
        return issues;
    }

    public long filesChecked() {
        return filesChecked;
    }

    public long rowsChecked() {
        return rowsChecked;
    }

    static final class Collector {
        private final List<ValidationIssue> issues = new ArrayList<>();
        private long filesChecked;
        private long rowsChecked;

        void error(String code, URI location, String message) {
            issues.add(
                    new ValidationIssue(ValidationIssue.Severity.ERROR, code, location, message));
        }

        void warning(String code, URI location, String message) {
            issues.add(
                    new ValidationIssue(ValidationIssue.Severity.WARNING, code, location, message));
        }

        void fileChecked() {
            filesChecked++;
        }

        void rowsChecked(long count) {
            rowsChecked = Math.addExact(rowsChecked, count);
        }

        ValidationReport build() {
            return new ValidationReport(issues, filesChecked, rowsChecked);
        }
    }
}
