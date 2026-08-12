# Java GraphAr performance matrix

This matrix reports only like-for-like workloads. It separates physical I/O from elapsed time
and never uses a full scan as a stand-in for a random-neighbor traversal.

## Cross-runtime topology scan: Java, C++, and Spark

All three runtimes scan the identical `ordered_by_source` topology of
`testing/ldbc_sample/parquet/ldbc_sample.graph.yml`: 6,626 `(src,dst)` rows in eleven adjacency
Parquet chunks (24,798 B on disk). They decode both ID columns and compute a checksum. There are
three warm-up scans before measurement; GraphInfo/reader creation and Spark session startup are
outside the timed region.

| Runtime and API | Timed scans | Measured time | Throughput |
| --- | ---: | ---: | ---: |
| Java `OrderedSourceEdgeReader.scanEdges()` | 10 | 55.880 ms/scan | 118,576 rows/s |
| C++ `AdjListArrowChunkReader` | Google Benchmark median of 5 repetitions | 2.480 ms/scan | 2,671,774 rows/s |
| Spark `EdgeReader.readAllAdjList(false).count()` | 10 | 131.687 ms/scan | 50,316 rows/s |

This is an API-level latency comparison, not a claim that the runtimes issue the same number of
filesystem reads: Java currently exposes exact storage counters, while the public C++ Arrow reader
and Spark `EdgeReader` do not. The shared payload is explicit above; each runtime additionally
performs its own Parquet footer/control-file work. Do not compare Spark startup to the other rows.

Run Java:

```bash
cd maven-projects
mvn --offline --no-transfer-progress -pl reader -am \
  -Dtest=TopologyScanBenchmarkTest -Dsurefire.failIfNoSpecifiedTests=false \
  -Dsurefire.useFile=false \
  -Dgraphar.bench.graph="$PWD/../testing/ldbc_sample/parquet/ldbc_sample.graph.yml" \
  test -Dspotless.check.skip=true
```

Run C++ after a `BUILD_BENCHMARKS=ON` CMake configure:

```bash
GRAPHAR_BENCH_GRAPH="$PWD/testing/ldbc_sample/parquet/ldbc_sample.graph.yml" \
  ./cpp-build/benchmarks/topology_scan_benchmark \
  --benchmark_min_time=0.15s --benchmark_repetitions=5 \
  --benchmark_report_aggregates_only=true
```

Run Spark with a JDK supported by the installed Spark version; its benchmark suite is opt-in through
the same `GRAPHAR_BENCH_GRAPH` environment variable.

## Indexed Parquet range read

The physical I/O test writes 262,144 generated rows with two `INT64` columns, then projects only
`payload` for rows `[131456, 131488)`. The timed test also writes the input file, so the stable
comparison is the storage work below rather than its 1.272 s JUnit elapsed time.

| Metric | Measured value |
| --- | ---: |
| Parquet file size | 4,226,329 B |
| Returned rows | 32 |
| Input bytes read | 12,210 B (0.289% of file) |
| Input opens | 1 |
| Seeks | 4 |
| `SeekableInput.read` calls | 3,587 |
| Test elapsed (write + indexed read) | 1.272 s |

The 0.289% read ratio demonstrates page-index selection rather than a full-file Java scan. The
small read-call count is a local-file observation; S3 uses bounded 64-KiB readahead to coalesce
these reads into range requests.

Run:

```bash
cd maven-projects
mvn --offline --no-transfer-progress -pl io-parquet -am \
  -Dtest=ParquetPhysicalIoEfficiencyTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dsurefire.useFile=false test \
  -Dspotless.check.skip=true
```

## Same-topology traversal: indexed Parquet versus off-heap Ignite CSR

Both paths use the same Java-written indexed GraphAr topology: 903 vertices and 6,626 edges.
Each result is a warmed three-hop expansion from vertex 297, with frontier sizes
`[1, 53, 219, 311]`, repeated 12 times. The two embedded Ignite nodes are localhost only, so this
is a reproducible implementation comparison, not a cross-host latency or capacity claim.

| Metric | Indexed GraphAr Parquet | Ignite CSR |
| --- | ---: | ---: |
| 12 traversals, total | 960.356 ms | 129.906 ms |
| Average per traversal | 80.030 ms | 10.826 ms |
| Relative latency | 7.39× slower | 7.39× faster |
| Query I/O opens | 262 | off-heap memory |
| Query I/O seeks | 1,026 | off-heap memory |
| Query I/O reads | 4,714 | off-heap memory |
| Query input bytes | 935,820 B | off-heap memory |
| Local storage I/O time | 28.908 ms | n/a |
| Java `PhysicalReader.read()` time | 846.682 ms | n/a |
| Java cursor `next()` time | 76.380 ms | n/a |
| Affinity jobs per hop | n/a | `[1, 7, 8]` |

The comparison identifies reader creation/page decode as the remaining Parquet cost: `PhysicalReader.read()`
accounts for 846.682 ms of the 960.356 ms total, while local file I/O accounts for 28.908 ms.

Run:

```bash
cd maven-projects
mvn --offline --no-transfer-progress -pl integration-ignite -am \
  -Dtest=IgniteCsrStoreIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dsurefire.useFile=false test \
  -Dspotless.check.skip=true
```

Measured on 2026-08-12 on the local development machine. Re-run before using the elapsed-time
numbers for a release comparison; the workload and I/O counters are the regression contract.
