# Delivery protocol

## Two delivery tracks

### Product fork (`origin`)

The fork is the primary product track and advances without waiting for Apache
review, issue triage, or upstream CI approval. Implement the complete
production vertical there as soon as its design and local verification are
ready. Keep commits small and layered as `upstreamable` or `product-only` so
the reusable core can later be extracted without a mega-squash.

### Apache upstream (`apache/incubator-graphar`)

Only an external upstream contribution is issue-first: create or identify the
focused issue before opening, updating, or requesting review on its PR.

1. Read the specification, research findings, existing discussions, and the
   applicable GitHub template before creating an issue.
2. Use the upstream issue template's required description and component
   fields; apply the appropriate type and `Component:*` labels.
3. State scope, non-goals, acceptance criteria, compatibility requirements,
   dependency links, and closure evidence. Start with umbrella issues; create
   component subissues only when their fork slice is ready for upstreaming.
4. Cross-link the issue, implementation branch, commits, PR, and design
   discussion. Use `Fixes #<issue>` only when the whole issue is completed;
   otherwise use `Relates to`.
5. Use the upstream pull-request template verbatim, complete its checklist,
   and use a Conventional Commit PR title such as `fix(java): ...`.
6. Before reporting an upstream PR as ready, inspect every check and its
   primary log. Resolve code failures; record fork-PR approval requirements
   separately from executed CI and never represent local verification as a
   green upstream workflow.

## Pure-Java SDK execution

The pure-Java SDK follows the documented architecture, format invariants,
compatibility contract, and roadmap in `docs/libraries/java/`. Treat C++ and
the canonical `testing/` fixtures as format-semantic oracles. Keep
`graphar-info` dependency-light; do not add physical storage, Parquet, Arrow,
or query-engine dependencies until metadata compatibility gates pass.
