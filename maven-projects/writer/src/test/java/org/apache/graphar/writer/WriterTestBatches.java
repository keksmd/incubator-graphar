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

package org.apache.graphar.writer;

import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.ValueVector;
import org.apache.graphar.io.VectorRecordBatch;

/** Columnar test input for writer scenarios. */
final class WriterTestBatches {
    private WriterTestBatches() {}

    static BatchCursor rows(Schema schema, List<Object[]> rows) {
        List<ValueVector> columns = new ArrayList<>(schema.fields().size());
        for (int column = 0; column < schema.fields().size(); column++) {
            Object[] values = new Object[rows.size()];
            for (int row = 0; row < rows.size(); row++) {
                Object[] valuesForRow = rows.get(row);
                if (valuesForRow == null || valuesForRow.length != schema.fields().size()) {
                    throw new IllegalArgumentException("Every test row must match the schema.");
                }
                values[row] = valuesForRow[column];
            }
            columns.add(new TestVector(schema.fields().get(column), values));
        }
        RecordBatch batch = new VectorRecordBatch(schema, columns, rows.size());
        return new BatchCursor() {
            private boolean available = true;

            @Override
            public boolean next() {
                boolean result = available;
                available = false;
                return result;
            }

            @Override
            public RecordBatch batch() {
                if (available) {
                    throw new IllegalStateException("Call next() before batch().");
                }
                return batch;
            }

            @Override
            public void close() {}
        };
    }

    private static final class TestVector implements ValueVector {
        private final Field field;
        private final Object[] values;

        private TestVector(Field field, Object[] values) {
            this.field = field;
            this.values = values;
        }

        @Override
        public Field field() {
            return field;
        }

        @Override
        public int valueCount() {
            return values.length;
        }

        @Override
        public boolean isNull(int index) {
            return values[index] == null;
        }

        @Override
        public Object getObject(int index) {
            return values[index];
        }
    }
}
