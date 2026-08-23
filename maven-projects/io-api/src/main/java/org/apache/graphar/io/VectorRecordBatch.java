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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** A validated {@link RecordBatch} backed by one vector for each schema field. */
public final class VectorRecordBatch implements RecordBatch {
    private final Schema schema;
    private final List<ValueVector> columns;
    private final int rowCount;

    /**
     * Creates a batch whose vectors have exactly {@code rowCount} values and match {@code schema}
     * by physical position.
     */
    public VectorRecordBatch(Schema schema, List<ValueVector> columns, int rowCount) {
        this.schema = Objects.requireNonNull(schema, "A batch schema cannot be null.");
        if (rowCount < 0) {
            throw new IllegalArgumentException("A batch row count cannot be negative.");
        }
        Objects.requireNonNull(columns, "Batch vectors cannot be null.");
        if (schema.fields().size() != columns.size()) {
            throw new IllegalArgumentException(
                    "A batch must contain one vector for every schema field.");
        }
        List<ValueVector> copy = new ArrayList<>(columns.size());
        for (int index = 0; index < columns.size(); index++) {
            ValueVector vector =
                    Objects.requireNonNull(columns.get(index), "A batch vector cannot be null.");
            if (!schema.fields().get(index).equals(vector.field())) {
                throw new IllegalArgumentException(
                        "A batch vector does not match its schema field.");
            }
            if (vector.valueCount() != rowCount) {
                throw new IllegalArgumentException(
                        "Every batch vector must have the batch row count.");
            }
            copy.add(vector);
        }
        this.columns = List.copyOf(copy);
        this.rowCount = rowCount;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public int rowCount() {
        return rowCount;
    }

    @Override
    public int columnCount() {
        return columns.size();
    }

    @Override
    public ValueVector column(int index) {
        return columns.get(index);
    }
}
