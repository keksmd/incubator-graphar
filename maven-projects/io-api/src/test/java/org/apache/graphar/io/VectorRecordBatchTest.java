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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class VectorRecordBatchTest {
    private static final Field IDS = new Field("id", ColumnType.of(ColumnType.Kind.INT64), false);
    private static final Field LABELS =
            new Field("label", ColumnType.of(ColumnType.Kind.STRING), true);

    @Test
    public void retainsVectorsInSchemaOrder() {
        Schema schema = new Schema(List.of(IDS, LABELS));
        ValueVector ids = new TestVector(IDS, List.of(7L, 8L));
        ValueVector labels = new TestVector(LABELS, Arrays.asList("a", null));

        RecordBatch batch = new VectorRecordBatch(schema, List.of(ids, labels), 2);

        assertEquals(2, batch.rowCount());
        assertEquals(2, batch.columnCount());
        assertSame(ids, batch.column(0));
        assertSame(labels, batch.column(1));
        assertEquals("a", batch.column(1).getObject(0));
        assertEquals(true, batch.column(1).isNull(1));
    }

    @Test
    public void rejectsStructuralMismatchesBeforePublishingTheBatch() {
        Schema schema = new Schema(List.of(IDS));
        ValueVector oneValue = new TestVector(IDS, List.of(7L));

        assertThrows(
                IllegalArgumentException.class,
                () -> new VectorRecordBatch(schema, List.of(oneValue), 2));
        assertThrows(
                IllegalArgumentException.class,
                () -> new VectorRecordBatch(schema, List.of(oneValue, oneValue), 1));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new VectorRecordBatch(
                                schema, List.of(new TestVector(LABELS, List.of("wrong"))), 1));
    }

    private static final class TestVector implements ValueVector {
        private final Field field;
        private final List<Object> values;

        private TestVector(Field field, List<Object> values) {
            this.field = field;
            this.values = values;
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
            return values.get(index);
        }
    }
}
