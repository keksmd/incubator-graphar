# GraphAr contributor context

## Pure-Java SDK

- The active Java implementation is `maven-projects/info`. Do not extend the deprecated FastFFI Java path when working on the new SDK.
- Treat the C++ implementation and canonical fixtures as the GraphAr format oracle; keep Java metadata paths and serialization compatible with them.
- Run Java verification in the declared development container when available:

  ```sh
  mvn --no-transfer-progress spotless:check
  mvn --no-transfer-progress clean verify -Dspotless.check.skip=true
  ```

## Delivery tracks

- `keksmd/incubator-graphar` is the product fork. It advances independently in coherent, tested slices and may temporarily lead Apache.
- `apache/incubator-graphar` is issue-first. Use its issue and PR templates, apply labels when permitted, and keep each PR a narrow implementation slice linked to its issue.
- Once upstream review starts, keep that PR's scope fixed: make only reviewer-requested changes relevant to its slice. Continue subsequent work in the fork.
- Promote only mature, standalone fork slices upstream at a measured cadence. Cut each upstream candidate from current `upstream/main` with the minimal needed commits; do not create an upstream PR for every fork commit or build a dependent PR chain.
- Never include `tasks/`, internal process records, or fork-only links in an Apache PR.
- Before every upstream commit or force-push, run `pre-commit run --files` on the exact changed paths. If a formatter edits files, rerun it until it passes; mark the PR checklist only with the hook result actually obtained.
- Preserve independently useful, in-flight fork layers while parallel sessions work ahead. Do not remove a prepared future boundary solely to shrink an earlier slice; keep it isolated, tested, and out of upstream promotion until its own scope is ready.

## IO contract boundary

- Keep storage limited to URI-backed files and seekable byte streams. Do not put GraphAr layout, Parquet, Hadoop, Arrow, projection, filtering, or query semantics into storage.
- The later `io-api` request must carry URI, projection, optional row range, optional predicate, and limit. A physical backend reports which hints it applied; declining a pushdown must not change results.
- Do not require Arrow as the public batch representation and do not add a general query AST before the reader vertical proves a need for it.
