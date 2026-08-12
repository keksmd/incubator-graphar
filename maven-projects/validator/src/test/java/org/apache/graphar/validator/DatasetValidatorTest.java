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

package org.apache.graphar.validator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Generated writer-backed validation and corruption matrix; no canonical fixture is used. */
public class DatasetValidatorTest {
    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void validatesEveryWriterLayoutByStreamingPhysicalFiles() throws Exception {
        TckDatasetExporter.GeneratedDataset graph = writeGeneratedGraph("valid");

        ValidationReport report = validate(graph);

        assertTrue(report.issues().toString(), report.isValid());
        assertTrue(report.filesChecked() > 20);
        assertTrue(report.rowsChecked() > 30);
    }

    @Test
    public void retainsTckDatasetAndGraphYamlOnlyWhenExplicitlyRequested() throws Exception {
        Path retained = TckDatasetExporter.retainedDirectory();
        if (retained == null) return;

        TckDatasetExporter.GeneratedDataset graph = TckDatasetExporter.write(retained);

        assertTrue(Files.isRegularFile(graph.graphYaml));
        ValidationReport report = validate(graph);
        assertTrue(report.issues().toString(), report.isValid());
    }

    @Test
    public void reportsGeneratedControlAndPropertyCorruptionWithoutThrowing() throws Exception {
        TckDatasetExporter.GeneratedDataset graph = writeGeneratedGraph("corrupt");
        Files.write(
                graph.path.resolve("vertex/person/vertex_count"),
                ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(7).array());
        Files.delete(
                graph.path.resolve(
                        "edge/person_knows_person/ordered_by_source/weight/part0/chunk0"));
        Path unorderedOffset =
                graph.path.resolve("edge/person_knows_person/unordered_by_dest/offset/chunk0");
        Files.createDirectories(unorderedOffset.getParent());
        Files.write(unorderedOffset, new byte[] {0});

        ValidationReport report = validate(graph);

        assertFalse(report.isValid());
        assertTrue(hasCode(report, "MISSING_FILE"));
        assertTrue(hasCode(report, "UNORDERED_OFFSET"));
    }

    private TckDatasetExporter.GeneratedDataset writeGeneratedGraph(String name) throws Exception {
        Path retained = TckDatasetExporter.retainedDirectory();
        Path output = temporaryFolder.newFolder(name).toPath();
        return TckDatasetExporter.write(output);
    }

    private static ValidationReport validate(TckDatasetExporter.GeneratedDataset graph) {
        return new DatasetValidator()
                .validate(
                        graph.info,
                        graph.root,
                        graph.storage,
                        new ParquetPhysicalReader(graph.storage));
    }

    private static boolean hasCode(ValidationReport report, String code) {
        return report.issues().stream().anyMatch(issue -> issue.code().equals(code));
    }
}
