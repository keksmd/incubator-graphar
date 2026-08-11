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

if [[ $# -ne 1 ]]; then
  echo "usage: $0 /path/to/test_info" >&2
  exit 2
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
test_info=$1
work_directory=$(mktemp -d "${TMPDIR:-/tmp}/graphar-java-cpp-metadata-tck.XXXXXX")
trap 'rm -rf "$work_directory"' EXIT

cpp_seed_directory="$work_directory/cpp-seed"
java_directory="$work_directory/java"
cpp_round_trip_directory="$work_directory/cpp-round-trip"
test_name="Load pure-Java metadata compatibility fixture"

GAR_METADATA_TCK_OUTPUT="$cpp_seed_directory" "$test_info" "$test_name"

(
  cd "$repo_root/maven-projects/info"
  mvn --no-transfer-progress \
    -Dtest=CrossLanguageMetadataCompatibilityTest \
    -Dgraphar.metadataTckInputDirectory="$cpp_seed_directory" \
    -Dgraphar.metadataTckOutputDirectory="$java_directory" \
    test
)

GAR_METADATA_TCK_INPUT="$java_directory" \
  GAR_METADATA_TCK_OUTPUT="$cpp_round_trip_directory" \
  "$test_info" "$test_name"

(
  cd "$repo_root/maven-projects/info"
  mvn --no-transfer-progress \
    -Dtest=CrossLanguageMetadataCompatibilityTest \
    -Dgraphar.metadataTckInputDirectory="$cpp_round_trip_directory" \
    test
)
