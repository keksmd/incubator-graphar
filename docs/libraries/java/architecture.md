---
id: java-architecture
title: Pure-Java SDK Architecture
sidebar_position: 3
---

# Pure-Java GraphAr SDK architecture

The legacy `maven-projects/java` FastFFI binding is deprecated. The pure-Java
SDK extends the existing `graphar-info` metadata module without copying the
C++ runtime or its dependency graph. C++ and Spark are format-semantic
oracles, not Java API templates.

## Design rules

1. `graphar-info` stays dependency-light: no Parquet, Hadoop, Arrow, or
   object-store dependency.
2. GraphAr layout/path resolution sits above physical file IO.
3. Physical readers receive projection, row-range, predicate, and limit hints;
   declined capabilities must be explicit and preserve results.
4. The graph API is not a query engine. Graph algorithms, Gremlin execution,
   and application caches stay outside SDK core.
5. IDs, counts, chunk indexes, and offsets are `long` values throughout.
6. Every supported capability has a cross-language fixture.

## Module boundaries

| Module | Responsibility | Must not depend on |
| --- | --- | --- |
| `graphar-info` | YAML metadata, schema, types, versions | storage and physical IO |
| `graphar-storage-api` | input/output files, seekable streams, capabilities | Parquet and GraphAr layout |
| `graphar-storage-local` | local-URI storage implementation | reader/writer |
| `graphar-io-api` | neutral batches and IO requests | GraphAr metadata semantics |
| `graphar-io-parquet` | Parquet projection/range/predicate execution | graph-facing API |
| `graphar-core` | chunk math and layout resolution | Parquet/Hadoop/Arrow |
| `graphar-reader` | vertex, property, adjacency, and bulk reads | writer/query engine |
| `graphar-writer` | chunking, offsets, properties, metadata publish | reader cache/query engine |
| `graphar-api` | stable graph-facing facade | physical backend implementation |

Hadoop, S3, Spark, TinkerPop, and Iceberg are adapters. They must not become
transitive requirements of `graphar-core` or `graphar-info`.

## Reader flow

```text
GraphInfo + EdgeInfo + requested adjacency layout
  -> GraphAr layout resolver
  -> offset chunk and [start, end) edge range
  -> ReadRequest(uri, projection, range, predicate, limit)
  -> physical backend
  -> cursor or primitive batch
```

The first reader vertical supports Parquet and `ordered_by_source`. It exposes
two distinct operations: `neighbors(vertexId)` resolves a small offset range;
`scanEdges()` streams source/destination batches. A `limit` is pushed down as
a hint rather than materializing a whole high-degree adjacency list in Java.

## Writer flow

```text
logical records -> ID assignment -> chunk partition -> layout ordering
-> offsets/property groups -> physical files -> validation -> metadata publish
```

Writers publish only after staged files validate and metadata is finalized.
This is a publish protocol, not a claim of distributed ACID transactions.

## First-release non-goals

* FastFFI/JNI and a C++ runtime.
* Custom Parquet or mandatory Arrow.
* General query AST, SQL/Gremlin engine, or graph algorithms.
* ORC/CSV, unordered layouts, cloud storage, and Iceberg in the first reader
  vertical.

The GraphAr specification is normative. C++ acts as an executable oracle for
ambiguous path, chunk, and offset behavior; Java compatibility fixtures make
each interpretation observable.
