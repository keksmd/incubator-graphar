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
