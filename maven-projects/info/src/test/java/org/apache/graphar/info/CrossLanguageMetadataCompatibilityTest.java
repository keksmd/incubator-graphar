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

package org.apache.graphar.info;

import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.info.saver.impl.LocalFileSystemYamlGraphSaver;
import org.apache.graphar.info.type.DataType;
import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * Proves the metadata bundle shared with the C++ compatibility test remains semantically stable.
 */
public class CrossLanguageMetadataCompatibilityTest {

    private static final String FIXTURE_DIRECTORY = "metadata-tck/";
    private static final String INPUT_DIRECTORY_PROPERTY = "graphar.metadataTckInputDirectory";
    private static final String OUTPUT_DIRECTORY_PROPERTY = "graphar.metadataTckOutputDirectory";

    @Test
    public void testCplusplusFixtureSurvivesJavaLoadAndSave() throws Exception {
        URI fixtureGraphUri = getFixtureGraphUri();
        LocalFileSystemStringGraphInfoLoader loader = new LocalFileSystemStringGraphInfoLoader();
        GraphInfo graphInfo = loader.loadGraphInfo(fixtureGraphUri);

        Assert.assertEquals(java.util.List.of("ldbc", "sample"), graphInfo.getLabels());
        Assert.assertEquals("test graph", graphInfo.getExtraInfo().get("category"));
        Assert.assertEquals(
                DataType.listOf(DataType.STRING),
                graphInfo.getVertexInfo("person").getPropertyType("emails"));

        Path outputDirectory = getOutputDirectory();
        graphInfo.setStoreUri(graphInfo.getVertexInfo("person"), URI.create("person.vertex.yaml"));
        graphInfo.setStoreUri(
                graphInfo.getEdgeInfo("person", "knows", "person"),
                URI.create("person_knows_person.edge.yaml"));
        URI outputGraphUri =
                URI.create(outputDirectory.resolve("ldbc_sample.graph.yaml").toString());
        new LocalFileSystemYamlGraphSaver().save(outputGraphUri, graphInfo);

        assertYamlEquals(fixtureGraphUri, outputGraphUri);
        assertYamlEquals(
                fixtureGraphUri.resolve("person.vertex.yaml"),
                outputDirectory.resolve("person.vertex.yaml").toUri());
        assertYamlEquals(
                fixtureGraphUri.resolve("person_knows_person.edge.yaml"),
                outputDirectory.resolve("person_knows_person.edge.yaml").toUri());
    }

    @Test
    public void testUnsupportedMetadataTypeFailsAtLoaderBoundary() throws Exception {
        URI invalidVertexUri =
                getClass()
                        .getClassLoader()
                        .getResource(FIXTURE_DIRECTORY + "unsupported_list.vertex.yaml")
                        .toURI();

        IllegalArgumentException error =
                Assert.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new LocalFileSystemStringGraphInfoLoader()
                                        .loadVertexInfo(invalidVertexUri));
        Assert.assertEquals("Unsupported GraphAr list value type: bool", error.getMessage());
    }

    private URI getFixtureGraphUri() throws Exception {
        String inputDirectory = System.getProperty(INPUT_DIRECTORY_PROPERTY);
        if (inputDirectory != null && !inputDirectory.isEmpty()) {
            return Path.of(inputDirectory).resolve("ldbc_sample.graph.yaml").toUri();
        }
        return getClass()
                .getClassLoader()
                .getResource(FIXTURE_DIRECTORY + "ldbc_sample.graph.yaml")
                .toURI();
    }

    private static Path getOutputDirectory() throws Exception {
        String outputDirectory = System.getProperty(OUTPUT_DIRECTORY_PROPERTY);
        if (outputDirectory == null || outputDirectory.isEmpty()) {
            return Files.createTempDirectory("graphar-java-metadata-tck-");
        }
        return Files.createDirectories(Path.of(outputDirectory));
    }

    @SuppressWarnings("unchecked")
    private static void assertYamlEquals(URI expectedUri, URI actualUri) throws Exception {
        Map<String, Object> expected = loadYaml(expectedUri);
        Map<String, Object> actual = loadYaml(actualUri);
        assertAdjacentListsEqual(expected.remove("adj_lists"), actual.remove("adj_lists"));
        Assert.assertEquals(expected, actual);
    }

    private static void assertAdjacentListsEqual(Object expected, Object actual) {
        if (expected == null && actual == null) {
            return;
        }
        Assert.assertEquals(new HashSet<>((List<?>) expected), new HashSet<>((List<?>) actual));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> loadYaml(URI uri) throws Exception {
        Path path = uri.getScheme() == null ? Path.of(uri.toString()) : Path.of(uri);
        try (Reader reader = Files.newBufferedReader(path)) {
            return new Yaml().load(reader);
        }
    }
}
