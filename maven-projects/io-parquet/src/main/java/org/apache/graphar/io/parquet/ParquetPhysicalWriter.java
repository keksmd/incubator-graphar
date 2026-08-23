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
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.graphar.io.BatchCursor;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.PhysicalWriter;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Schema;
import org.apache.graphar.io.ValueVector;
import org.apache.graphar.io.WriteMode;
import org.apache.graphar.io.WriteRequest;
import org.apache.graphar.storage.Storage;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/** A storage-backed writer for flat, primitive Parquet batches. */
public final class ParquetPhysicalWriter implements PhysicalWriter {
    private static final int INDEXED_PAGE_ROW_COUNT = 1024;

    private final Storage storage;

    /** Creates a writer that resolves every output URI through {@code storage}. */
    public ParquetPhysicalWriter(Storage storage) {
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    @Override
    public void write(WriteRequest request, BatchCursor batches) throws IOException {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(batches, "batches");
        if (request.mode() == WriteMode.APPEND) {
            throw new UnsupportedOperationException(
                    "ParquetPhysicalWriter cannot append to an existing Parquet file.");
        }
        MessageType parquetSchema = parquetSchema(request.schema());
        ParquetFileWriter.Mode mode =
                request.mode() == WriteMode.CREATE_NEW
                        ? ParquetFileWriter.Mode.CREATE
                        : ParquetFileWriter.Mode.OVERWRITE;
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                new ParquetOutputFile(storage.outputFile(request.uri())))
                        .withType(parquetSchema)
                        .withWriteMode(mode)
                        .withPageRowCountLimit(INDEXED_PAGE_ROW_COUNT)
                        .build()) {
            SimpleGroupFactory groups = new SimpleGroupFactory(parquetSchema);
            while (batches.next()) {
                RecordBatch batch =
                        Objects.requireNonNull(batches.batch(), "batch cursor returned null");
                requireSchema(request.schema(), batch.schema());
                List<ValueVector> columns = requireColumns(request.schema(), batch);
                for (int rowIndex = 0; rowIndex < batch.rowCount(); rowIndex++) {
                    writer.write(toGroup(groups, request.schema(), columns, rowIndex));
                }
            }
        } finally {
            batches.close();
        }
    }

    private static MessageType parquetSchema(Schema schema) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (Field field : schema.fields()) {
            if (field.type().kind() == ColumnType.Kind.LIST) {
                builder.addField(listType(field));
                continue;
            }
            Types.PrimitiveBuilder<Types.GroupBuilder<MessageType>> primitive =
                    builder.primitive(physicalType(field.type()), repetition(field));
            LogicalTypeAnnotation logicalType = logicalType(field.type());
            if (logicalType != null) {
                primitive.as(logicalType);
            }
            primitive.named(field.name());
        }
        return builder.named("graphar");
    }

    private static Type listType(Field field) {
        ColumnType element = field.type().elementType().orElseThrow();
        Type value = listElementType(element);
        return (field.nullable() ? Types.optionalList() : Types.requiredList())
                .element(value)
                .named(field.name());
    }

    private static Type listElementType(ColumnType element) {
        LogicalTypeAnnotation logical = logicalType(element);
        if (logical == null) {
            return Types.repeated(physicalType(element)).named("element");
        }
        return Types.repeated(physicalType(element)).as(logical).named("element");
    }

    private static PrimitiveType.PrimitiveTypeName physicalType(ColumnType type) {
        switch (type.kind()) {
            case BOOLEAN:
                return PrimitiveType.PrimitiveTypeName.BOOLEAN;
            case INT8:
            case INT16:
            case INT32:
            case DATE:
                return PrimitiveType.PrimitiveTypeName.INT32;
            case INT64:
            case TIMESTAMP_MILLIS:
                return PrimitiveType.PrimitiveTypeName.INT64;
            case FLOAT32:
                return PrimitiveType.PrimitiveTypeName.FLOAT;
            case FLOAT64:
                return PrimitiveType.PrimitiveTypeName.DOUBLE;
            case STRING:
            case BINARY:
                return PrimitiveType.PrimitiveTypeName.BINARY;
            case LIST:
            default:
                throw new IllegalArgumentException("Unsupported flat Parquet type: " + type.kind());
        }
    }

    private static Type.Repetition repetition(Field field) {
        return field.nullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
    }

    private static LogicalTypeAnnotation logicalType(ColumnType type) {
        switch (type.kind()) {
            case INT8:
                return LogicalTypeAnnotation.intType(8, true);
            case INT16:
                return LogicalTypeAnnotation.intType(16, true);
            case DATE:
                return LogicalTypeAnnotation.dateType();
            case TIMESTAMP_MILLIS:
                return LogicalTypeAnnotation.timestampType(
                        true, LogicalTypeAnnotation.TimeUnit.MILLIS);
            case STRING:
                return LogicalTypeAnnotation.stringType();
            default:
                return null;
        }
    }

    private static void requireSchema(Schema expected, Schema actual) {
        List<Field> expectedFields = expected.fields();
        List<Field> actualFields = actual.fields();
        if (expectedFields.size() != actualFields.size()) {
            throw new IllegalArgumentException("Record batch schema does not match write request.");
        }
        for (int index = 0; index < expectedFields.size(); index++) {
            Field left = expectedFields.get(index);
            Field right = actualFields.get(index);
            if (!left.equals(right)) {
                throw new IllegalArgumentException(
                        "Record batch schema does not match write request.");
            }
        }
    }

    private static List<ValueVector> requireColumns(Schema schema, RecordBatch batch) {
        if (batch.columnCount() != schema.fields().size()) {
            throw new IllegalArgumentException("Record batch vectors do not match write request.");
        }
        List<ValueVector> columns = new ArrayList<>(batch.columnCount());
        for (int index = 0; index < batch.columnCount(); index++) {
            ValueVector column =
                    Objects.requireNonNull(
                            batch.column(index), "record batch vector cannot be null");
            if (!schema.fields().get(index).equals(column.field())
                    || column.valueCount() != batch.rowCount()) {
                throw new IllegalArgumentException(
                        "Record batch vectors do not match write request.");
            }
            columns.add(column);
        }
        return columns;
    }

    private static Group toGroup(
            SimpleGroupFactory groups, Schema schema, List<ValueVector> columns, int rowIndex) {
        Group group = groups.newGroup();
        for (int index = 0; index < schema.fields().size(); index++) {
            Field field = schema.fields().get(index);
            ValueVector column = columns.get(index);
            boolean nullValue = column.isNull(rowIndex);
            Object value = column.getObject(rowIndex);
            if (nullValue != (value == null)) {
                throw new IllegalArgumentException(
                        "Vector nullness does not match its value: " + field.name());
            }
            if (nullValue) {
                if (!field.nullable()) {
                    throw new IllegalArgumentException("Required field is null: " + field.name());
                }
                continue;
            }
            add(group, field, value);
        }
        return group;
    }

    private static void add(Group group, Field field, Object value) {
        String name = field.name();
        switch (field.type().kind()) {
            case BOOLEAN:
                group.add(name, require(value, Boolean.class, name));
                return;
            case INT8:
            case INT16:
            case INT32:
                group.add(name, require(value, Number.class, name).intValue());
                return;
            case INT64:
                group.add(name, require(value, Number.class, name).longValue());
                return;
            case FLOAT32:
                group.add(name, require(value, Number.class, name).floatValue());
                return;
            case FLOAT64:
                group.add(name, require(value, Number.class, name).doubleValue());
                return;
            case STRING:
                group.add(name, Binary.fromString(require(value, String.class, name)));
                return;
            case BINARY:
                ByteBuffer bytes = require(value, ByteBuffer.class, name).asReadOnlyBuffer();
                byte[] copy = new byte[bytes.remaining()];
                bytes.get(copy);
                group.add(name, Binary.fromConstantByteArray(copy));
                return;
            case DATE:
                group.add(
                        name, Math.toIntExact(require(value, LocalDate.class, name).toEpochDay()));
                return;
            case TIMESTAMP_MILLIS:
                group.add(name, require(value, Instant.class, name).toEpochMilli());
                return;
            case LIST:
                addList(group, field, value);
                return;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Parquet type: " + field.type().kind());
        }
    }

    private static void addList(Group group, Field field, Object value) {
        if (!(value instanceof List<?>)) {
            throw new IllegalArgumentException(
                    "Unexpected value for " + field.name() + ": expected List");
        }
        Group list = group.addGroup(field.name());
        Field element = new Field("element", field.type().elementType().orElseThrow(), false);
        for (Object elementValue : (List<?>) value) {
            if (elementValue == null) {
                throw new IllegalArgumentException(
                        "Parquet LIST elements cannot be null: " + field.name());
            }
            add(list.addGroup("list"), element, elementValue);
        }
    }

    private static <T> T require(Object value, Class<T> type, String name) {
        if (!type.isInstance(value)) {
            throw new IllegalArgumentException(
                    "Unexpected value for " + name + ": expected " + type.getSimpleName());
        }
        return type.cast(value);
    }
}
