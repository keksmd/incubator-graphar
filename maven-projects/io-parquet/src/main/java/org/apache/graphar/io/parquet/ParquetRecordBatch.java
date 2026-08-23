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

package org.apache.graphar.io.parquet;

import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.ValueVector;
import org.apache.graphar.io.VectorRecordBatch;

/** Immutable vectors materialized from one Parquet row group. */
final class ParquetRecordBatch implements RecordBatch {
    private final VectorRecordBatch delegate;

    ParquetRecordBatch(Schema schema, List<? extends List<?>> columns, int rowCount) {
        if (schema.fields().size() != columns.size()) {
            throw new IllegalArgumentException("Parquet batch vectors do not match schema.");
        }
        List<ValueVector> vectors = new ArrayList<>(columns.size());
        for (int index = 0; index < columns.size(); index++) {
            vectors.add(new ParquetValueVector(schema.fields().get(index), columns.get(index)));
        }
        this.delegate = new VectorRecordBatch(schema, vectors, rowCount);
    }

    @Override
    public Schema schema() {
        return delegate.schema();
    }

    @Override
    public int rowCount() {
        return delegate.rowCount();
    }

    @Override
    public int columnCount() {
        return delegate.columnCount();
    }

    @Override
    public ValueVector column(int index) {
        return delegate.column(index);
    }
}
