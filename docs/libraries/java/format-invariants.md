---
id: java-format-invariants
title: Pure-Java Format Invariants
sidebar_position: 4
---

# Pure-Java GraphAr format invariants

This is the implementation contract for the Java SDK. It supplements the
[GraphAr format specification](/docs/specification/format).

## Metadata and identity

* A graph is `graph.yml` plus referenced vertex and edge metadata. References
  resolve relative to the graph metadata URI.
* Vertex types use labels; edge types use the `(source, edge, destination)`
  triplet.
* Load/save preserves prefixes, file types, chunk sizes, versions, nullability,
  property types, labels, and extra metadata.
* GraphAr format version and Java SDK version are independent.

## IDs and chunks

* Internal vertex IDs are non-negative, zero-based `long` values.
* A vertex chunk index is `vertexId / vertexChunkSize`; local position is
  `vertexId % vertexChunkSize`.
* Edge data is first partitioned by source or destination vertex chunk, then
  divided into edge chunks. Chunk counts round up; final chunks may be partial.
* Source-oriented layouts use `src_chunk_size`; destination-oriented layouts
  use `dst_chunk_size`.
* Range endpoints are half-open: `[begin, end)`.

## Layouts and offsets

| Layout | Partition key | Ordering | Offset table |
| --- | --- | --- | --- |
| `ordered_by_source` | source ID | source ordered | required |
| `ordered_by_dest` | destination ID | destination ordered | required |
| `unordered_by_source` | source ID | unordered | absent |
| `unordered_by_dest` | destination ID | unordered | absent |

For every ordered vertex partition of size `V`, its offset chunk has `V + 1`
`INT64` values. It begins at zero; values are non-negative and monotonic; the
range of local vertex `i` is `[offset[i], offset[i + 1])`; the final value
equals that partition's edge count. Ordered-source and ordered-destination are
storage-level CSR-like and CSC-like layouts respectively, not a promise of one
fully materialized JVM array.

## Canonical relative paths

The C++ `VertexInfo` and `EdgeInfo` methods are the executable oracle. Physical
file names have no extension; metadata `file_type` selects the decoder.

```text
vertex property: {vertex.prefix}/{property.prefix}/chunk{vertexChunk}
vertex count:    {vertex.prefix}/vertex_count

edge vertex count: {edge.prefix}/{adj.prefix}/vertex_count
edge count:        {edge.prefix}/{adj.prefix}/edge_count{vertexChunk}
adjacency:         {edge.prefix}/{adj.prefix}/adj_list/part{vertexChunk}/chunk{edgeChunk}
offset:            {edge.prefix}/{adj.prefix}/offset/chunk{vertexChunk}
edge property:     {edge.prefix}/{adj.prefix}/{property.prefix}/part{vertexChunk}/chunk{edgeChunk}
```

No Java API may omit adjacency type, vertex chunk index, or edge chunk index
when resolving edge topology/property chunks. Path joining normalizes separators
and handles a dataset root plus relative or absolute overrides; raw
`URI.resolve` with an unchecked non-trailing-slash base is insufficient.

## Properties and validation

* Property-group rows align with their vertex or edge rows.
* Nulls are preserved; Java defaults must not replace physical nulls.
* Unsupported property types fail before returning a partial result.
* Primary properties are never nullable.
* Validation checks referenced files, counts, offset semantics, ID bounds,
  ordered layout order, row alignment, schemas, and supported format/file type.

Every invariant gets a fixture under `testing/`. A Java writer is accepted only
after both `C++/Spark write -> Java read` and `Java write -> C++/Spark read`
work for the declared capability.
