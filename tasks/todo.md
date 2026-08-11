# Java library index

- [x] Confirm the actively developed pure-Java module and exclude the deprecated FFI module.
- [x] Rebuild the AST index for the repository and verify its Java symbol coverage.
- [x] Record the resulting module map and verification evidence.

## Review

Indexed `/Users/alex/IdeaProjects/incubator-graphar/maven-projects/info` as a standalone project: 47 files, 870 symbols, 6,216 references, and one Maven module. Verified outline and references for `GraphInfo`, plus implementations of the loader and saver interfaces. The FastFFI module at `maven-projects/java` was not included in this focused index.

# Development environment and verification

- [x] Inspect the devcontainer definition and start its development environment.
- [x] Build the focused `graphar-info` pure-Java module in the devcontainer.
- [x] Run the focused module's Maven test suite and record results.

## Review

The published devcontainer image `ghcr.io/apache/graphar-dev:latest` is cached locally and was verified as Linux/arm64 with OpenJDK 11.0.24 and Maven 3.6.3. The required `testing` submodule was initialized. Inside this image, `mvn --no-transfer-progress clean verify -Dspotless.check.skip=true` completed successfully in 3m38s: 123 tests passed with 0 failures, errors, and skips; the binary JAR, Javadoc JAR, and JaCoCo XML were produced. `spotless:check` and `javadoc:javadoc` also passed in a JDK 11 Maven container; Javadoc emitted two missing-tag warnings in `VersionInfo.checkType`.

# Pure-Java SDK bootstrap

- [x] Phase 0: publish the architecture, compatibility contract, format invariants, and delivery roadmap.
- [x] Phase 1a: restore C++-compatible edge path resolution in `graphar-info` and replace self-referential URI tests with fixture parity checks.
- [ ] Phase 1b: close [#944](https://github.com/apache/incubator-graphar/issues/944) with cross-language metadata fixtures and remaining verified `graphar-info` gaps; it depends on edge-layout gate [#943](https://github.com/apache/incubator-graphar/issues/943).
- [ ] Phase 2: introduce dependency-light storage and physical-IO APIs with local storage.
- [ ] Phase 3: implement the Parquet projection/range backend.
- [ ] Phase 4: implement the ordered-by-source reader vertical: layout, offsets, adjacency, `neighbors`, and `scanEdges`.
- [ ] Phase 5: implement the writer, validator, remaining layouts, and integrations in independently releasable slices.
- [ ] Prepare—but do not externally post—the #756/design-discussion proposal and upstream PR sequence.

## Review

Phase 0 documents live under `docs/libraries/java/` and are linked from the Java library overview. The upstream delivery chain is issue-first: architecture discussion [#756](https://github.com/apache/incubator-graphar/issues/756), edge-layout gate [#943](https://github.com/apache/incubator-graphar/issues/943), and metadata parity [#944](https://github.com/apache/incubator-graphar/issues/944). Phase 1a makes `EdgeInfo` C++-compatible: counts and offsets resolve below the adjacency prefix, topology resolves as `adj_list/part{vertexChunk}/chunk{edgeChunk}`, and edge properties resolve below the selected adjacency layout with the same tuple. The test suite proves these suffixes against the real Parquet fixture, including `part2/chunk1`, and covers prefixes without trailing slashes. The resulting `mvn clean verify` in the devcontainer passed 124 tests with zero failures, errors, or skips.
