# Delivery protocol

## Issue-first GitHub workflow

For every code change, create or identify a focused upstream issue before
opening, updating, or requesting review on a pull request.

1. Read the relevant specification, research findings, and existing upstream
   discussion before creating an issue.
2. The issue must state scope, non-goals, acceptance criteria, format/API
   compatibility requirements, and test evidence required for closure.
3. Cross-link the issue, implementation branch, commits, pull request, and
   design-discussion comments. Use `Fixes #<issue>` only when the whole issue
   is actually completed by that pull request; otherwise use `Relates to`.
4. Do not open a code PR until its issue exists. Keep work on the branch while
   issue-first planning or investigation is incomplete.
5. Before completion, inspect every PR check; fix failures from their primary
   logs and confirm the final required checks are green.
6. PR titles must use the repository's conventional-commit release type, for
   example `fix:`, `feat:`, `docs:`, `test:`, or `refactor:`; a bracketed
   component prefix alone fails the mandatory title check.

## Pure-Java SDK execution

The pure-Java SDK follows the documented architecture, format invariants,
compatibility contract, and roadmap in `docs/libraries/java/`. Treat C++ and
the canonical `testing/` fixtures as format-semantic oracles. Keep
`graphar-info` dependency-light; do not add physical storage, Parquet, Arrow,
or query-engine dependencies until metadata compatibility gates pass.
