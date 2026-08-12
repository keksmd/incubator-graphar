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

package org.apache.graphar

import org.apache.graphar.graph.GraphReader
import org.apache.graphar.reader.{EdgeReader, VertexReader}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

/**
 * Consumer contract for a dataset emitted by the pure-Java GraphWriter.
 *
 * No fixture is bundled here: point this suite at an independently generated
 * dataset with `GRAPHAR_JAVA_TCK_GRAPH` or
 * `-Dgraphar.java.tck.graph=<graph.yml>`. This makes the producer and the Spark
 * consumer separate build artifacts.
 */
class JavaWriterSparkConsumerTckSuite
    extends AnyFunSuite
    with BeforeAndAfterAll {

  private val graphYaml =
    Option(System.getProperty("graphar.java.tck.graph"))
      .orElse(Option(System.getenv("GRAPHAR_JAVA_TCK_GRAPH")))
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    if (graphYaml.nonEmpty) {
      spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("graphar-java-writer-spark-tck")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
      spark.sparkContext.setLogLevel("Error")
    }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  test(
    "reads metadata and every Java-writer layout through the public Spark API"
  ) {
    val graphPath = graphYaml.getOrElse {
      cancel(
        "Set GRAPHAR_JAVA_TCK_GRAPH or -Dgraphar.java.tck.graph to a graph.yml " +
          "written by the Java GraphWriter."
      )
    }
    val graphInfo = GraphInfo.loadGraphInfo(graphPath, spark)
    val activeSpark = spark
    import activeSpark.implicits._

    // Literal producer-independent oracle. The Java TCK exporter emits these
    // values; the Spark reader must reproduce the complete multiset, not just
    // a compatible schema or row count.
    val expectedVertices = Seq(
      (0L, "person-0"),
      (1L, "person-1"),
      (2L, "person-2"),
      (3L, "person-3"),
      (4L, "person-4"),
      (5L, "person-5")
    ).toDF(GeneralParams.vertexIndexCol, "name")
    val expectedEdges = Seq(
      (0L, 1L, 0.5d),
      (5L, 4L, 1.5d),
      (4L, 1L, 2.5d),
      (3L, 4L, 3.5d),
      (2L, 1L, 4.5d),
      (1L, 4L, 5.5d),
      (0L, 1L, 6.5d),
      (5L, 4L, 7.5d),
      (4L, 1L, 8.5d),
      (3L, 4L, 9.5d),
      (2L, 1L, 10.5d),
      (1L, 4L, 11.5d),
      (0L, 1L, 12.5d),
      (5L, 4L, 13.5d),
      (4L, 1L, 14.5d),
      (3L, 4L, 15.5d),
      (2L, 1L, 16.5d)
    ).toDF(GeneralParams.srcIndexCol, GeneralParams.dstIndexCol, "weight")

    graphInfo.getVertexInfos.foreach { case (vertexType, vertexInfo) =>
      val vertexReader =
        new VertexReader(graphInfo.getPrefix, vertexInfo, spark)
      val rawVertexFrame = vertexReader.readAllVertexPropertyGroups()
      requireVertexIndex(vertexType, rawVertexFrame.columns, graphPath)

      val declaredProperties = vertexInfo.getProperty_groups.asScala
        .flatMap(_.getProperties.asScala.map(_.getName))
        .toSet
      assert(
        declaredProperties.subsetOf(rawVertexFrame.columns.toSet),
        s"Spark vertex reader lost declared properties for '$vertexType': " +
          s"expected $declaredProperties, actual ${rawVertexFrame.columns.toSet}"
      )
      assert(
        rawVertexFrame.count() == vertexReader.readVerticesNumber(),
        s"Spark vertex row count differs from vertex_count for '$vertexType'."
      )
      assertSameMultiset(
        expectedVertices,
        rawVertexFrame.select(GeneralParams.vertexIndexCol, "name"),
        s"VertexReader values for '$vertexType'"
      )
    }

    val (vertices, layouts) = GraphReader.read(graphPath, spark)
    graphInfo.getVertexInfos.foreach { case (vertexType, vertexInfo) =>
      val frame = vertices(vertexType)
      requireVertexIndex(vertexType, frame.columns, graphPath)
      val expected =
        new VertexReader(graphInfo.getPrefix, vertexInfo, spark)
          .readVerticesNumber()
      assert(
        frame.count() == expected,
        s"GraphReader returned an incomplete '$vertexType' frame."
      )
      assertSameMultiset(
        expectedVertices,
        frame.select(GeneralParams.vertexIndexCol, "name"),
        s"GraphReader vertex values for '$vertexType'"
      )
    }

    graphInfo.getEdgeInfos.foreach { case (_, edgeInfo) =>
      val key = (
        edgeInfo.getSrc_type(),
        edgeInfo.getEdge_type(),
        edgeInfo.getDst_type()
      )
      val consumerLayouts = layouts.getOrElse(
        key,
        fail(s"GraphReader did not expose Java-writer edge '$key'.")
      )
      edgeInfo.getAdj_lists.asScala.foreach { adjacentList =>
        val layout = adjacentList.getAdjList_type_in_gar
        val layoutName = adjacentList.getAdjList_type
        val frame = consumerLayouts.getOrElse(
          layoutName,
          fail(
            s"GraphReader did not expose Java-writer layout '$layoutName' for '$key'."
          )
        )
        val expected =
          new EdgeReader(graphInfo.getPrefix, edgeInfo, layout, spark)
            .readEdgesNumber()
        assert(
          frame.count() == expected,
          s"Spark edge count differs for '$key/$layoutName'."
        )
        assert(
          Set(GeneralParams.srcIndexCol, GeneralParams.dstIndexCol)
            .subsetOf(frame.columns.toSet),
          s"Spark edge schema for '$key/$layoutName' is missing topology columns: " +
            frame.columns.mkString(", ")
        )
        assert(
          frame.columns.contains("weight"),
          s"Spark edge schema for '$key/$layoutName' is missing weight."
        )
        assertSameMultiset(
          expectedEdges,
          frame.select(
            GeneralParams.srcIndexCol,
            GeneralParams.dstIndexCol,
            "weight"
          ),
          s"GraphReader topology/property values for '$key/$layoutName'"
        )
      }
    }
  }

  private def assertSameMultiset(
      expected: DataFrame,
      actual: DataFrame,
      description: String
  ): Unit = {
    assert(
      expected.exceptAll(actual).count() == 0,
      s"$description omitted or changed rows."
    )
    assert(
      actual.exceptAll(expected).count() == 0,
      s"$description added or changed rows."
    )
  }

  private def requireVertexIndex(
      vertexType: String,
      columns: Array[String],
      graphPath: String
  ): Unit = {
    assert(
      columns.contains(GeneralParams.vertexIndexCol),
      s"Java-writer Spark TCK rejected '$graphPath': vertex '$vertexType' is missing " +
        s"${GeneralParams.vertexIndexCol}. Spark combines vertex property groups and labels by " +
        "this stable physical ID; Java GraphWriter must persist it in every vertex property " +
        "group before this dataset can be consumed safely."
    )
  }
}
