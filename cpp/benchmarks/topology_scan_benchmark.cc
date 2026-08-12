// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>

#include "benchmark/benchmark.h"

#include "arrow/api.h"
#include "graphar/api/arrow_reader.h"
#include "graphar/api/info.h"
#include "graphar/fwd.h"
#include "graphar/reader_util.h"

namespace graphar {

static void ScanOrderedSourceTopology(::benchmark::State& state) {  // NOLINT
  const char* graph_path = std::getenv("GRAPHAR_BENCH_GRAPH");
  if (graph_path == nullptr || *graph_path == '\0') {
    state.SkipWithError(
        "set GRAPHAR_BENCH_GRAPH to an absolute GraphAr graph YAML path");
    return;
  }
  auto maybe_graph_info = GraphInfo::Load(graph_path);
  if (!maybe_graph_info.status().ok()) {
    state.SkipWithError(maybe_graph_info.status().message().c_str());
    return;
  }
  const auto& graph_info = maybe_graph_info.value();
  const auto edge_info = graph_info->GetEdgeInfo("person", "knows", "person");
  if (edge_info == nullptr) {
    state.SkipWithError("person_knows_person edge metadata is missing");
    return;
  }
  auto maybe_reader = AdjListArrowChunkReader::Make(
      graph_info, "person", "knows", "person", AdjListType::ordered_by_source);
  if (!maybe_reader.status().ok()) {
    state.SkipWithError(maybe_reader.status().message().c_str());
    return;
  }
  const auto& reader = maybe_reader.value();

  for (auto _ : state) {
    int64_t rows = 0;
    int64_t checksum = 0;
    for (IdType vertex_chunk = 0; vertex_chunk < 10; ++vertex_chunk) {
      auto maybe_chunk_count =
          util::GetEdgeChunkNum(graph_info->GetPrefix(), edge_info,
                                AdjListType::ordered_by_source, vertex_chunk);
      if (!maybe_chunk_count.status().ok()) {
        state.SkipWithError(maybe_chunk_count.status().message().c_str());
        return;
      }
      for (IdType edge_chunk = 0; edge_chunk < maybe_chunk_count.value();
           ++edge_chunk) {
        const auto seek_partition =
            reader->seek_chunk_index(vertex_chunk, edge_chunk);
        if (!seek_partition.ok()) {
          state.SkipWithError(seek_partition.message().c_str());
          return;
        }
        auto maybe_table = reader->GetChunk();
        if (!maybe_table.status().ok()) {
          state.SkipWithError(maybe_table.status().message().c_str());
          return;
        }
        const auto& table = maybe_table.value();
        if (table != nullptr) {
          const auto source = std::static_pointer_cast<arrow::Int64Array>(
              table->GetColumnByName(GeneralParams::kSrcIndexCol)->chunk(0));
          const auto destination = std::static_pointer_cast<arrow::Int64Array>(
              table->GetColumnByName(GeneralParams::kDstIndexCol)->chunk(0));
          for (int64_t index = 0; index < table->num_rows(); ++index) {
            checksum += source->Value(index) * 31 + destination->Value(index);
          }
          rows += table->num_rows();
        }
      }
    }
    if (rows != 6626) {
      state.SkipWithError("expected 6626 LDBC topology rows");
      return;
    }
    benchmark::DoNotOptimize(checksum);
  }
  state.SetItemsProcessed(state.iterations() * 6626);
}

BENCHMARK(ScanOrderedSourceTopology)->Unit(::benchmark::kMillisecond);

}  // namespace graphar
