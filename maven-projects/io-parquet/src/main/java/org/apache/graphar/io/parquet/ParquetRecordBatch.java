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

import java.util.List;
import org.apache.graphar.io.RecordBatch;
import org.apache.graphar.io.Row;
import org.apache.graphar.io.Schema;

/** Immutable rows materialized from one Parquet row group. */
final class ParquetRecordBatch implements RecordBatch {
    private final Schema schema;
    private final List<ParquetRow> rows;

    ParquetRecordBatch(Schema schema, List<ParquetRow> rows) {
        this.schema = schema;
        this.rows = List.copyOf(rows);
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public int rowCount() {
        return rows.size();
    }

    @Override
    public Row row(int index) {
        return rows.get(index);
    }
}
