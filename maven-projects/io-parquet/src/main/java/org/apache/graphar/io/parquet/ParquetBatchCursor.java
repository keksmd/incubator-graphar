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

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ComparisonOperator;
import org.apache.graphar.io.Filter;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.RowRange;
import org.apache.graphar.io.Schema;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

/** Streams materialized Parquet row groups as neutral record batches. */
final class ParquetBatchCursor implements BatchCursor {
    private final ParquetFileReader fileReader;
    private final MessageType fileSchema;
    private final MessageType readSchema;
    private final List<ParquetColumn> readColumns;
    private final Schema outputSchema;
    private final List<Filter> filters;
    private final Map<String, Integer> readColumnIndexes;
    private final int[] outputIndexes;
    private final long rangeStart;
    private final long rangeEnd;
    private final long limit;
    private long sourceRow;
    private long emitted;
    private boolean exhausted;
    private boolean closed;
    private RecordBatch current;

    ParquetBatchCursor(
            ParquetFileReader fileReader,
            MessageType fileSchema,
            MessageType readSchema,
            List<ParquetColumn> readColumns,
            List<ParquetColumn> outputColumns,
            Schema outputSchema,
            ReadRequest request) {
        this.fileReader = fileReader;
        this.fileSchema = fileSchema;
        this.readSchema = readSchema;
        this.readColumns = readColumns;
        this.outputSchema = outputSchema;
        this.filters = request.filters();
        this.readColumnIndexes = indexes(readColumns);
        this.outputIndexes = outputIndexes(outputColumns, readColumnIndexes);
        RowRange range = request.rowRange().orElse(null);
        this.rangeStart = range == null ? 0 : range.startInclusive();
        this.rangeEnd = range == null ? Long.MAX_VALUE : range.endExclusive();
        this.limit = request.limit().isPresent() ? request.limit().getAsLong() : Long.MAX_VALUE;
    }

    @Override
    public boolean next() throws IOException {
        if (closed || exhausted) {
            current = null;
            return false;
        }
        if (emitted == limit) {
            finish();
            return false;
        }
        try {
            while (true) {
                PageReadStore pages = fileReader.readNextRowGroup();
                if (pages == null) {
                    finish();
                    return false;
                }
                try {
                    long groupStart = sourceRow;
                    long rowCount = pages.getRowCount();
                    sourceRow = Math.addExact(sourceRow, rowCount);
                    if (groupStart >= rangeEnd || sourceRow <= rangeStart) {
                        continue;
                    }
                    MessageColumnIO columnIO =
                            new ColumnIOFactory().getColumnIO(readSchema, fileSchema);
                    RecordReader<Group> rows =
                            columnIO.getRecordReader(pages, new GroupRecordConverter(readSchema));
                    List<ParquetRow> matched = new ArrayList<>();
                    for (long index = 0; index < rowCount; index++) {
                        Group group = rows.read();
                        long rowIndex = groupStart + index;
                        if (rowIndex < rangeStart || rowIndex >= rangeEnd) {
                            continue;
                        }
                        Object[] values = values(group);
                        if (!matches(values)) {
                            continue;
                        }
                        matched.add(new ParquetRow(project(values)));
                        emitted++;
                        if (emitted == limit) {
                            exhausted = true;
                            break;
                        }
                    }
                    if (!matched.isEmpty()) {
                        current = new ParquetRecordBatch(outputSchema, matched);
                        if (exhausted) {
                            closeReader();
                        }
                        return true;
                    }
                    if (exhausted) {
                        finish();
                        return false;
                    }
                } finally {
                    pages.close();
                }
            }
        } catch (IOException | RuntimeException exception) {
            try {
                closeReader();
            } catch (IOException closeException) {
                exception.addSuppressed(closeException);
            }
            throw exception;
        }
    }

    @Override
    public RecordBatch batch() {
        if (current == null) {
            throw new IllegalStateException("No current batch. Call next() before batch().");
        }
        return current;
    }

    @Override
    public void close() throws IOException {
        current = null;
        exhausted = true;
        closeReader();
    }

    private Object[] values(Group group) {
        Object[] values = new Object[readColumns.size()];
        for (int index = 0; index < readColumns.size(); index++) {
            if (group.getFieldRepetitionCount(index) != 0) {
                values[index] = value(group, index, readColumns.get(index));
            }
        }
        return values;
    }

    private static Object value(Group group, int index, ParquetColumn column) {
        switch (column.field().type().kind()) {
            case BOOLEAN:
                return group.getBoolean(index, 0);
            case INT8:
                return (byte) group.getInteger(index, 0);
            case INT16:
                return (short) group.getInteger(index, 0);
            case INT32:
                return group.getInteger(index, 0);
            case INT64:
                return group.getLong(index, 0);
            case FLOAT32:
                return group.getFloat(index, 0);
            case FLOAT64:
                return group.getDouble(index, 0);
            case STRING:
                return group.getBinary(index, 0).toStringUsingUTF8();
            case BINARY:
                return group.getBinary(index, 0).getBytes();
            case DATE:
                return LocalDate.ofEpochDay(group.getInteger(index, 0));
            case TIMESTAMP_MILLIS:
                return Instant.ofEpochMilli(group.getLong(index, 0));
            default:
                throw new IllegalArgumentException(
                        "Unsupported Parquet column type: " + column.field().type().kind());
        }
    }

    private boolean matches(Object[] values) {
        for (Filter filter : filters) {
            Object value = values[readColumnIndexes.get(filter.column())];
            if (!matches(value, filter)) {
                return false;
            }
        }
        return true;
    }

    private static boolean matches(Object value, Filter filter) {
        if (filter.operator() == ComparisonOperator.IS_NULL) {
            return value == null;
        }
        if (filter.operator() == ComparisonOperator.IS_NOT_NULL) {
            return value != null;
        }
        if (value == null) {
            return false;
        }
        int comparison = compare(value, filter.value().value());
        switch (filter.operator()) {
            case EQUAL:
                return comparison == 0;
            case NOT_EQUAL:
                return comparison != 0;
            case LESS_THAN:
                return comparison < 0;
            case LESS_THAN_OR_EQUAL:
                return comparison <= 0;
            case GREATER_THAN:
                return comparison > 0;
            case GREATER_THAN_OR_EQUAL:
                return comparison >= 0;
            default:
                throw new IllegalArgumentException(
                        "Unsupported comparison operator: " + filter.operator());
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compare(Object value, Object literal) {
        if (value instanceof Boolean) {
            return value.equals(literal) ? 0 : 1;
        }
        return ((Comparable) value).compareTo(literal);
    }

    private Object[] project(Object[] values) {
        Object[] output = new Object[outputIndexes.length];
        for (int index = 0; index < outputIndexes.length; index++) {
            Object value = values[outputIndexes[index]];
            output[index] = value instanceof byte[] ? ((byte[]) value).clone() : value;
        }
        return output;
    }

    private void finish() throws IOException {
        exhausted = true;
        current = null;
        closeReader();
    }

    private void closeReader() throws IOException {
        if (!closed) {
            closed = true;
            fileReader.close();
        }
    }

    private static Map<String, Integer> indexes(List<ParquetColumn> columns) {
        Map<String, Integer> indexes = new HashMap<>();
        for (int index = 0; index < columns.size(); index++) {
            indexes.put(columns.get(index).field().name(), index);
        }
        return indexes;
    }

    private static int[] outputIndexes(
            List<ParquetColumn> outputColumns, Map<String, Integer> readColumnIndexes) {
        int[] indexes = new int[outputColumns.size()];
        for (int index = 0; index < outputColumns.size(); index++) {
            indexes[index] = readColumnIndexes.get(outputColumns.get(index).field().name());
        }
        return indexes;
    }
}
