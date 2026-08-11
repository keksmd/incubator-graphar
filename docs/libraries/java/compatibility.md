---
id: java-compatibility
title: Pure-Java Compatibility Contract
sidebar_position: 5
---

# Pure-Java GraphAr compatibility contract

Java compatibility means that Java, C++, and Spark preserve the same GraphAr
meaning. It does not require matching object models or runtime dependencies.

## Release 0.1 target matrix

| Capability | Java | C++ | Spark | Gate |
| --- | --- | --- | --- | --- |
| Graph metadata load/save | target | reference | reference | semantic YAML round trip |
| Vertex metadata/property groups | target | reference | reference | labels, paths, types, nullability |
| Edge metadata/layouts | target | reference | reference | triplets and chunk semantics |
| Parquet vertices/properties | target | reference | reference | projected fixture read/write |
| `ordered_by_source` | target | reference | reference | offset range and neighbors |
| `ordered_by_dest` | after 0.1 core | reference | reference | incoming neighbors |
| Unordered layouts | deferred | reference | reference | 0.2 gate |
| Local storage | target | reference | reference | file URI fixture |
| Hadoop/S3 storage | deferred | reference | reference | adapter integration test |

`target` becomes a release promise only when a corresponding fixture and gate
exist. Unsupported layouts and types fail fast; they are not emulated through a
different physical layout.

## Initial compatibility slice

Before adding a physical reader, `graphar-info` must agree with C++ on suffixes
for vertex property/count, edge vertex/edge counts, offset chunks, topology
chunks, and edge property chunks. Tests use the Parquet fixture under
`testing/ldbc_sample/parquet`, not self-derived URI expectations.

The slice also covers relative and absolute paths, prefixes without a trailing
slash, all four adjacency declarations, empty partitions, and a nonzero
`part{vertexChunk}/chunk{edgeChunk}` pair.

## Golden datasets and tests

The suite grows from canonical datasets: `empty`, `boundary-chunks`,
`properties`, `nulls`, `multi-property-group`, ordered/unordered source and
destination, and `high-degree`.

Test levels are unit (math/path), format (external fixture), round trip, local
backend integration, and performance. The first reader vertical proves both
`neighbors(vertexId)` and sequential `scanEdges()`; passing after reading all
edge data does not establish the random-read contract.

Before release, tests verify no metadata loss, no nullability/row-alignment
loss, equal neighbor sets and ranges, `long`-safe counts/offsets, and external
reader acceptance of Java writer output.
