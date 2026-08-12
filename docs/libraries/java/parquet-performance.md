# Java Parquet physical-I/O contract

This document records the reproducible I/O baseline and the acceptance criteria for the
pure-Java GraphAr Parquet path. It is deliberately measured in bytes and storage operations,
not elapsed time: wall-clock throughput varies with the JVM, disk cache, network, and S3
endpoint, while reading an unnecessary page or issuing an unnecessary object request is a
deterministic regression.

## Baseline: indexed projected range read

Run the generated regression scenario from `maven-projects/`:

```bash
mvn --offline --no-transfer-progress -pl io-parquet -am \
  -Dtest=ParquetPhysicalIoEfficiencyTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dsurefire.useFile=false test \
  -Dspotless.check.skip=true
```

The scenario writes 262,144 generated rows with two required `INT64` columns. The payload is a
deterministic high-entropy permutation, so the result is materially sized rather than an
artificially dictionary-compressible example. It then projects only `payload` and reads 32 rows
beginning at row 131,456. The source cursor generates rows in 1,024-row batches and does not
retain the data set in a collection.

Baseline captured on 2026-08-12 with Parquet Java 1.18.0:

| Quantity | Observed value |
| --- | ---: |
| Parquet file bytes | 4,226,329 |
| Write bytes | 4,226,329 |
| Write calls to `PositionOutput` | 7,908 |
| Projected rows returned | 32 |
| Input bytes read | 12,210 (0.289% of file) |
| `SeekableInput` opens | 1 |
| Seeks | 4 |
| `SeekableInput.read` calls | 3,587 |

The byte result proves that the reader reaches the selected indexed page instead of decoding the
whole file. It is consistent with Parquet's Page Index design: an `OffsetIndex` navigates pages
by row index, and pages are the indivisible unit of compression/encoding. See the
[Parquet Page Index specification](https://parquet.apache.org/docs/file-format/pageindex/) and
[Parquet data-page configuration guidance](https://parquet.apache.org/docs/file-format/configurations/).

## Bottlenecks established by the baseline

1. The local reader makes 3,587 small `SeekableInput.read` calls while navigating Parquet
   metadata. `S3SeekableInput` coalesces adjacent reads into a 64-KiB pinned range buffer, so
   these calls do not become one HTTP `GetObject` request each. Its contract test performs 10,000
   adjacent single-byte reads with one range GET.
2. `ParquetPhysicalWriter` writes one Java `Group` per logical row. The generated scenario makes
   7,908 calls to the storage output, although the S3 writer stages them locally and publishes a
   single object on close. This is CPU/allocation work, not an S3 request-count problem.
3. A Parquet page is the smallest compressed read unit. `ParquetBatchCursor` keeps the selected
   pages open but emits at most 1,024 materialized rows per `RecordBatch`; a full scan no longer
   retains a complete row group in Java heap.

## Optimization acceptance criteria

### S3 selective reads

For the exact generated scenario above, with a recording S3 client:

- one `HeadObject` maximum per opened file;
- at most 8 `GetObject` range requests for the projected 32-row read;
- at most 256 KiB of `GetObject` response bytes; and
- the returned 32 values must remain identical to the generated sequence.

The 64-KiB bounded readahead buffer in `S3SeekableInput` serves small sequential Parquet stream
reads from one fetched range. It is position-aware after every `seek`, clamps at object end,
preserves the pinned version/ETag precondition, and never returns bytes outside the current
position. The criteria are intentionally request/byte based, so they are valid for MinIO and AWS
S3 without a timing threshold.

### Local and remote scan memory

The physical cursor must expose bounded batches instead of building a list proportional to an
entire selected row group. The acceptance test should use at least one 1,000,000-row row group
and demonstrate that the maximum materialized `RecordBatch` is no larger than the configured
batch size while a full projection scan returns every row exactly once. This is a correctness and
bounded-allocation contract; heap sampling may be recorded as supporting evidence but is not a CI
gate.

### Writer throughput and memory

The writer must continue to publish one complete Parquet object per GraphAr chunk, preserve
Offset Indexes for every projected topology column, and consume the caller's `BatchCursor` in
bounded batches. A later CPU benchmark may compare `Group` construction with a direct low-level
writer, but it is not accepted merely on a wall-clock result: it must retain the same Parquet
schema, LIST/null semantics, page indexes, and Java-to-Java read results.

## Regression guard

`ParquetPhysicalIoEfficiencyTest` is part of the regular `io-parquet` test suite. It uses a
counting storage wrapper, asserts exactly one input stream, verifies the returned range values,
and fails if that read consumes one eighth or more of the generated file. It does not compare
elapsed time and therefore is suitable for CI.
