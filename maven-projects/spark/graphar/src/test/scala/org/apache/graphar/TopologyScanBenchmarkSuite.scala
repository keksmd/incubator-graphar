/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy
 * of the License at
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

import org.apache.graphar.reader.EdgeReader
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Optional warmed topology-only scan benchmark matching Java and C++ harnesses.
 */
class TopologyScanBenchmarkSuite extends AnyFunSuite with BeforeAndAfterAll {
  private val graphYaml = Option(System.getenv("GRAPHAR_BENCH_GRAPH"))
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    if (graphYaml.nonEmpty) {
      spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("graphar-topology-scan-benchmark")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
      spark.sparkContext.setLogLevel("Error")
    }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  test("scans canonical topology") {
    val graphPath = graphYaml.getOrElse(
      cancel("set GRAPHAR_BENCH_GRAPH to a GraphAr graph YAML")
    )
    val graphInfo = GraphInfo.loadGraphInfo(graphPath, spark)
    val edgeInfo = graphInfo.getEdgeInfo("person", "knows", "person")
    val reader = new EdgeReader(
      graphInfo.getPrefix,
      edgeInfo,
      AdjListType.ordered_by_source,
      spark
    )
    val warmup = 3
    val iterations = 10
    for (_ <- 0 until warmup)
      assert(reader.readAllAdjList(false).count() == 6626)
    val started = System.nanoTime()
    var rows = 0L
    for (_ <- 0 until iterations) rows += reader.readAllAdjList(false).count()
    val elapsedNanos = System.nanoTime() - started
    assert(rows == 66260)
    println(
      s"SPARK_TOPOLOGY_SCAN rows=6626 warmup=$warmup iterations=$iterations " +
        s"total_ms=${elapsedNanos / 1000000.0} avg_ms=${elapsedNanos / 1000000.0 / iterations}"
    )
  }
}
