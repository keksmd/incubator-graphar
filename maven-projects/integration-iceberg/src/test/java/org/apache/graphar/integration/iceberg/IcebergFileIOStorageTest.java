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

package org.apache.graphar.integration.iceberg;

import java.net.URI;
import java.nio.ByteBuffer;
import org.apache.graphar.storage.PositionOutput;
import org.apache.graphar.storage.SeekableInput;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.junit.Assert;
import org.junit.Test;

/** Proves GraphAr physical IO preserves Iceberg FileIO write and seek semantics. */
public class IcebergFileIOStorageTest {
    @Test
    public void adaptsIcebergFileIOWithoutFilesystemOrCloudCredentials() throws Exception {
        InMemoryFileIO fileIO = new InMemoryFileIO();
        IcebergFileIOStorage storage = new IcebergFileIOStorage(fileIO);
        URI uri = URI.create("s3://graphar/export/chunk0");

        try (PositionOutput output = storage.outputFile(uri).create()) {
            output.write(new byte[] {4, 5, 6, 7});
            Assert.assertEquals(4, output.position());
        }
        Assert.assertTrue(storage.exists(uri));
        Assert.assertEquals(4, storage.inputFile(uri).size());
        try (SeekableInput input = storage.inputFile(uri).open()) {
            input.seek(1);
            ByteBuffer bytes = ByteBuffer.allocate(2);
            Assert.assertEquals(2, input.read(bytes));
            Assert.assertArrayEquals(new byte[] {5, 6}, bytes.array());
        }
    }
}
