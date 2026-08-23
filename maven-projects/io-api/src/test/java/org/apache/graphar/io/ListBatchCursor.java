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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** An in-memory reference {@link BatchCursor} used to exercise the neutral batch contract. */
final class ListBatchCursor implements BatchCursor {
    private final Schema schema;
    private final List<List<List<Object>>> batches;
    private int index = -1;
    private int closeCount;

    ListBatchCursor(Schema schema, List<List<List<Object>>> batches) {
        this.schema = Objects.requireNonNull(schema);
        this.batches = batches;
    }

    @Override
    public boolean next() {
        if (index + 1 >= batches.size()) {
            return false;
        }
        index++;
        return true;
    }

    @Override
    public RecordBatch batch() {
        if (index < 0 || index >= batches.size()) {
            throw new IllegalStateException("No batch is current.");
        }
        return new ListRecordBatch(schema, batches.get(index));
    }

    @Override
    public void close() {
        closeCount++;
    }

    int closeCount() {
        return closeCount;
    }

    private static final class ListRecordBatch implements RecordBatch {
        private final VectorRecordBatch delegate;

        private ListRecordBatch(Schema schema, List<List<Object>> rows) {
            List<List<Object>> valuesByColumn = new ArrayList<>(schema.fields().size());
            for (int column = 0; column < schema.fields().size(); column++) {
                valuesByColumn.add(new ArrayList<>(rows.size()));
            }
            for (List<Object> row : rows) {
                Objects.requireNonNull(row, "A batch row cannot be null.");
                if (row.size() != schema.fields().size()) {
                    throw new IllegalArgumentException(
                            "A batch row does not match the schema width.");
                }
                for (int column = 0; column < row.size(); column++) {
                    valuesByColumn.get(column).add(row.get(column));
                }
            }
            List<ValueVector> columns = new ArrayList<>(valuesByColumn.size());
            for (int column = 0; column < valuesByColumn.size(); column++) {
                columns.add(
                        new ListValueVector(
                                schema.fields().get(column), valuesByColumn.get(column)));
            }
            this.delegate = new VectorRecordBatch(schema, columns, rows.size());
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
        public ValueVector column(int columnIndex) {
            return delegate.column(columnIndex);
        }
    }

    private static final class ListValueVector implements ValueVector {
        private final Field field;
        private final List<Object> values;

        private ListValueVector(Field field, List<Object> values) {
            this.field = field;
            this.values = Collections.unmodifiableList(new ArrayList<>(values));
        }

        @Override
        public Field field() {
            return field;
        }

        @Override
        public int valueCount() {
            return values.size();
        }

        @Override
        public boolean isNull(int index) {
            return values.get(index) == null;
        }

        @Override
        public Object getObject(int index) {
            Object value = values.get(index);
            return value instanceof List
                    ? Collections.unmodifiableList(new ArrayList<>((List<?>) value))
                    : value;
        }
    }
}
