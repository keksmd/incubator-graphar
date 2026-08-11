---
id: java-roadmap
title: Pure-Java SDK Roadmap
sidebar_position: 6
---

# Pure-Java GraphAr SDK roadmap

## Delivery model

The product fork moves independently of upstream review. Commits remain small,
tested, and layered so reusable parts can be proposed upstream. With explicit
authorization, upstream delivery is issue-first: an issue states the scope,
acceptance evidence, and dependencies before any corresponding code PR opens.

## Milestones

| Phase | Deliverable | Exit evidence |
| --- | --- | --- |
| 0 | architecture, invariants, compatibility, roadmap | these pages checked against local spec/C++ |
| 1 | `graphar-info` URI/type/metadata parity fixtures and fixes | C++ suffix parity, C++ -> Java -> C++ metadata round trip |
| 2 | storage API and local implementation | core has no physical-format dependency |
| 3 | IO request/capability API and Parquet backend | projection/range tests report used capabilities |
| 4 | ordered-source reader | graph.yml -> offsets -> Parquet -> `neighbors` and `scanEdges` |
| 5 | writer and validator vertical | Java write <-> C++/Spark read gates |
| 6 | remaining layouts and adapters | expanded matrix and benchmarks |

## First code slice

The first upstreamable PR fixes the existing Java metadata contract before new
modules exist:

1. Add C++-equivalent edge URI methods for counts, offsets, adjacency, and
   edge properties.
2. Require `(adjacency type, vertex chunk, edge chunk)` for topology/property
   chunks.
3. Replace incompatible Java URI expectations with C++/fixture parity tests.
4. Add a nonzero `part/chunk` and no-trailing-slash path test.

Only then does the first reader vertical begin:

```text
GraphAr v1 metadata + local storage + Parquet + ordered_by_source
  -> offset range -> destination projection -> neighbors(vertexId)
  -> sequential src/dst scanEdges()
```

## Upstream sequence

1. Compatibility fixtures and focused `graphar-info` fixes.
2. IO-neutral storage interfaces and local storage.
3. IO request/capability API.
4. Parquet projection reader.
5. Chunk/layout resolution, offsets, adjacency, reader facade.
6. Writer, validator, remaining layouts, and adapters.

Upstream tracking is issue-first: [#756](https://github.com/apache/incubator-graphar/issues/756)
records the architecture discussion, [#943](https://github.com/apache/incubator-graphar/issues/943)
tracks the edge-layout compatibility gate, and
[#944](https://github.com/apache/incubator-graphar/issues/944) tracks the
remaining metadata-parity gate. Subsequent delivery gates are storage
[#947](https://github.com/apache/incubator-graphar/issues/947), IO contract
[#948](https://github.com/apache/incubator-graphar/issues/948), Parquet
[#949](https://github.com/apache/incubator-graphar/issues/949), ordered-source
reader [#950](https://github.com/apache/incubator-graphar/issues/950), and
writer/validator [#951](https://github.com/apache/incubator-graphar/issues/951).
A code PR links its issue and its completed acceptance evidence.
