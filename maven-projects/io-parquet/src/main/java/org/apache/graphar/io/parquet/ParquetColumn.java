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

import org.apache.graphar.io.Field;
import org.apache.parquet.schema.PrimitiveType;

/** One supported Parquet primitive and its neutral field contract. */
final class ParquetColumn {
    private final PrimitiveType parquetType;
    private final Field field;

    ParquetColumn(PrimitiveType parquetType, Field field) {
        this.parquetType = parquetType;
        this.field = field;
    }

    PrimitiveType parquetType() {
        return parquetType;
    }

    Field field() {
        return field;
    }
}
