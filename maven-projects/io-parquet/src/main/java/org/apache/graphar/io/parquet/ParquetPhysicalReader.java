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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.graphar.io.ColumnType;
import org.apache.graphar.io.Field;
import org.apache.graphar.io.Filter;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.ReadCapability;
import org.apache.graphar.io.ReadReport;
import org.apache.graphar.io.ReadRequest;
import org.apache.graphar.io.ReadResult;
import org.apache.graphar.io.Schema;
import org.apache.graphar.storage.InputFile;
import org.apache.graphar.storage.Storage;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/** A storage-backed reader for flat, primitive Parquet files. */
public final class ParquetPhysicalReader implements PhysicalReader {
    private static final Set<ReadCapability> CAPABILITIES =
            Collections.unmodifiableSet(
                    EnumSet.of(ReadCapability.PROJECTION, ReadCapability.LIMIT));

    private final Storage storage;

    /** Creates a reader that resolves each request URI through {@code storage}. */
    public ParquetPhysicalReader(Storage storage) {
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    @Override
    public Set<ReadCapability> capabilities() {
        return CAPABILITIES;
    }

    @Override
    public ReadResult read(ReadRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        InputFile inputFile =
                Objects.requireNonNull(storage.inputFile(request.uri()), "storage inputFile");
        ParquetFileReader fileReader = null;
        try {
            fileReader = ParquetFileReader.open(new ParquetInputFile(inputFile));
            MessageType fileSchema = fileReader.getFooter().getFileMetaData().getSchema();
            List<ParquetColumn> fileColumns = columns(fileSchema);
            Map<String, ParquetColumn> columnsByName = byName(fileColumns);
            List<ParquetColumn> outputColumns = outputColumns(request, fileColumns, columnsByName);
            validateFilters(request.filters(), columnsByName);
            List<ParquetColumn> readColumns =
                    readColumns(request, fileColumns, outputColumns, columnsByName);
            MessageType readSchema =
                    new MessageType(fileSchema.getName(), parquetTypes(readColumns));
            fileReader.setRequestedSchema(readSchema);

            Schema outputSchema = new Schema(fields(outputColumns));
            ReadReport report = new ReadReport(applied(request), declined(request));
            ParquetBatchCursor cursor =
                    new ParquetBatchCursor(
                            fileReader,
                            fileSchema,
                            readSchema,
                            readColumns,
                            outputColumns,
                            outputSchema,
                            request);
            fileReader = null;
            return new ReadResult(request, cursor, report);
        } finally {
            if (fileReader != null) {
                fileReader.close();
            }
        }
    }

    private static List<ParquetColumn> columns(MessageType fileSchema) {
        List<ParquetColumn> columns = new ArrayList<>();
        for (Type type : fileSchema.getFields()) {
            if (!type.isPrimitive() || type.isRepetition(Type.Repetition.REPEATED)) {
                throw new IllegalArgumentException(
                        "Only flat, non-repeated primitive Parquet columns are supported: "
                                + type.getName());
            }
            columns.add(new ParquetColumn(type.asPrimitiveType(), toField(type.asPrimitiveType())));
        }
        return List.copyOf(columns);
    }

    private static Map<String, ParquetColumn> byName(List<ParquetColumn> columns) {
        Map<String, ParquetColumn> columnsByName = new HashMap<>();
        for (ParquetColumn column : columns) {
            if (columnsByName.put(column.field().name(), column) != null) {
                throw new IllegalArgumentException(
                        "Duplicate Parquet column: " + column.field().name());
            }
        }
        return columnsByName;
    }

    private static List<ParquetColumn> outputColumns(
            ReadRequest request,
            List<ParquetColumn> fileColumns,
            Map<String, ParquetColumn> columnsByName) {
        if (request.projection().isAllColumns()) {
            return fileColumns;
        }
        List<ParquetColumn> result = new ArrayList<>();
        for (String name : request.projection().columns()) {
            ParquetColumn column = columnsByName.get(name);
            if (column == null) {
                throw new IllegalArgumentException("Unknown projection column: " + name);
            }
            result.add(column);
        }
        return List.copyOf(result);
    }

    private static List<ParquetColumn> readColumns(
            ReadRequest request,
            List<ParquetColumn> fileColumns,
            List<ParquetColumn> outputColumns,
            Map<String, ParquetColumn> columnsByName) {
        Map<String, ParquetColumn> needed = new LinkedHashMap<>();
        for (ParquetColumn column : outputColumns) {
            needed.put(column.field().name(), column);
        }
        for (Filter filter : request.filters()) {
            needed.put(filter.column(), columnsByName.get(filter.column()));
        }
        List<ParquetColumn> result = new ArrayList<>();
        for (ParquetColumn column : fileColumns) {
            if (needed.containsKey(column.field().name())) {
                result.add(column);
            }
        }
        return List.copyOf(result);
    }

    private static List<Type> parquetTypes(List<ParquetColumn> columns) {
        List<Type> types = new ArrayList<>();
        for (ParquetColumn column : columns) {
            types.add(column.parquetType());
        }
        return types;
    }

    private static List<Field> fields(List<ParquetColumn> columns) {
        List<Field> fields = new ArrayList<>();
        for (ParquetColumn column : columns) {
            fields.add(column.field());
        }
        return fields;
    }

    private static Set<ReadCapability> applied(ReadRequest request) {
        EnumSet<ReadCapability> applied = EnumSet.noneOf(ReadCapability.class);
        if (!request.projection().isAllColumns()) {
            applied.add(ReadCapability.PROJECTION);
        }
        if (request.limit().isPresent()) {
            applied.add(ReadCapability.LIMIT);
        }
        return applied;
    }

    private static Set<ReadCapability> declined(ReadRequest request) {
        EnumSet<ReadCapability> declined = EnumSet.noneOf(ReadCapability.class);
        if (request.rowRange().isPresent()) {
            declined.add(ReadCapability.ROW_RANGE);
        }
        if (!request.filters().isEmpty()) {
            declined.add(ReadCapability.FILTER);
        }
        return declined;
    }

    private static Field toField(PrimitiveType type) {
        return new Field(type.getName(), type(type), !type.isRepetition(Type.Repetition.REQUIRED));
    }

    private static ColumnType type(PrimitiveType type) {
        LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
        if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT32, "DATE");
            return ColumnType.of(ColumnType.Kind.DATE);
        }
        if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp =
                    (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logical;
            requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT64, "TIMESTAMP");
            if (timestamp.getUnit() != LogicalTypeAnnotation.TimeUnit.MILLIS) {
                throw unsupported(type, "only millisecond timestamps are supported");
            }
            return ColumnType.of(ColumnType.Kind.TIMESTAMP_MILLIS);
        }
        if (logical instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
            LogicalTypeAnnotation.IntLogicalTypeAnnotation integer =
                    (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logical;
            if (!integer.isSigned()) {
                throw unsupported(type, "unsigned integer logical types are not supported");
            }
            switch (integer.getBitWidth()) {
                case 8:
                    requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT32, "INT(8)");
                    return ColumnType.of(ColumnType.Kind.INT8);
                case 16:
                    requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT32, "INT(16)");
                    return ColumnType.of(ColumnType.Kind.INT16);
                case 32:
                    requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT32, "INT(32)");
                    return ColumnType.of(ColumnType.Kind.INT32);
                case 64:
                    requirePhysical(type, PrimitiveType.PrimitiveTypeName.INT64, "INT(64)");
                    return ColumnType.of(ColumnType.Kind.INT64);
                default:
                    throw unsupported(type, "unsupported integer bit width");
            }
        }
        if (logical instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
            if (type.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.BINARY
                    && type.getPrimitiveTypeName()
                            != PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                throw unsupported(type, "STRING must use BINARY or FIXED_LEN_BYTE_ARRAY");
            }
            return ColumnType.of(ColumnType.Kind.STRING);
        }
        if (logical != null) {
            throw unsupported(type, "unsupported logical type " + logical);
        }
        switch (type.getPrimitiveTypeName()) {
            case BOOLEAN:
                return ColumnType.of(ColumnType.Kind.BOOLEAN);
            case INT32:
                return ColumnType.of(ColumnType.Kind.INT32);
            case INT64:
                return ColumnType.of(ColumnType.Kind.INT64);
            case FLOAT:
                return ColumnType.of(ColumnType.Kind.FLOAT32);
            case DOUBLE:
                return ColumnType.of(ColumnType.Kind.FLOAT64);
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return ColumnType.of(ColumnType.Kind.BINARY);
            default:
                throw unsupported(type, "unsupported primitive type");
        }
    }

    private static void requirePhysical(
            PrimitiveType type, PrimitiveType.PrimitiveTypeName expected, String logicalName) {
        if (type.getPrimitiveTypeName() != expected) {
            throw unsupported(type, logicalName + " must use " + expected);
        }
    }

    private static IllegalArgumentException unsupported(PrimitiveType type, String reason) {
        return new IllegalArgumentException(
                "Unsupported Parquet column " + type.getName() + ": " + reason);
    }

    static void validateFilters(List<Filter> filters, Map<String, ParquetColumn> columnsByName) {
        for (Filter filter : filters) {
            ParquetColumn column = columnsByName.get(filter.column());
            if (column == null) {
                throw new IllegalArgumentException("Unknown filter column: " + filter.column());
            }
            ColumnType.Kind kind = column.field().type().kind();
            switch (filter.operator()) {
                case IS_NULL:
                case IS_NOT_NULL:
                    continue;
                default:
                    break;
            }
            if (kind == ColumnType.Kind.BINARY || kind == ColumnType.Kind.LIST) {
                throw new IllegalArgumentException(
                        "Filter comparisons are unsupported for " + kind);
            }
            if (kind == ColumnType.Kind.BOOLEAN
                    && filter.operator() != org.apache.graphar.io.ComparisonOperator.EQUAL
                    && filter.operator() != org.apache.graphar.io.ComparisonOperator.NOT_EQUAL) {
                throw new IllegalArgumentException(
                        "Boolean filters only support equality comparisons.");
            }
            if (!literalClass(kind).isInstance(filter.value().value())) {
                throw new IllegalArgumentException(
                        "Filter literal for "
                                + filter.column()
                                + " must be "
                                + literalClass(kind).getSimpleName());
            }
        }
    }

    private static Class<?> literalClass(ColumnType.Kind kind) {
        switch (kind) {
            case BOOLEAN:
                return Boolean.class;
            case INT8:
                return Byte.class;
            case INT16:
                return Short.class;
            case INT32:
                return Integer.class;
            case INT64:
                return Long.class;
            case FLOAT32:
                return Float.class;
            case FLOAT64:
                return Double.class;
            case STRING:
                return String.class;
            case DATE:
                return LocalDate.class;
            case TIMESTAMP_MILLIS:
                return Instant.class;
            default:
                throw new IllegalArgumentException(
                        "Filter comparisons are unsupported for " + kind);
        }
    }
}
