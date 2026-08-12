#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 /path/to/test_info /absolute/path/to/java-generated.graph.yml" >&2
  exit 2
fi

test_info=$1
graph_path=$2

if [[ ! -x "$test_info" ]]; then
  echo "C++ test binary is not executable: $test_info" >&2
  echo "Required setup: configure cpp with BUILD_TESTS=ON, then build test_info." >&2
  exit 2
fi
if [[ ! -f "$graph_path" ]]; then
  echo "Java-generated graph YAML does not exist: $graph_path" >&2
  echo "Required setup: write and retain a graph with GraphWriter.writeMetadata first." >&2
  exit 2
fi

graph_directory=$(cd "$(dirname "$graph_path")" && pwd)
graph_path="$graph_directory/$(basename "$graph_path")"

GRAPHAR_JAVA_TCK_GRAPH="$graph_path" \
  "$test_info" "Load generated Java Parquet graph through C++ readers"
