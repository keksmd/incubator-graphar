# Lessons

- Treat newly mentioned workspace files as in-scope task inputs. Do not label an unreviewed untracked file as unrelated work; inspect it before reporting its scope.
- When the user authorizes plan execution and GitHub access, continue with the highest-priority ready slice and create the needed GitHub issues, pull requests, and comments without announcing a proposed next step or waiting for approval.
- Treat GitHub PR checks as required verification: inspect every failed run, use its exact log to fix the root cause, push the repair, and confirm the checks are green before reporting the slice as complete.
- Keep the product fork and Apache upstream as separate delivery tracks. The fork advances independently with production-ready vertical slices; issue-first, labels, templates, review, and Actions approval apply to external upstream issues and PRs only. Start upstream with the plan's umbrella issues, then create subissues only when a corresponding fork slice is ready.
- Never include internal `tasks/` files, delivery-process documents, or fork-only cross-links in an Apache PR. Before pushing an upstream branch, compare its file list to the linked issue and remove every non-implementation path; keep those records on the product-fork branch only.
- After upstream review begins, treat that PR as a fixed scope boundary: apply only feedback that belongs to its existing slice. Keep building and integrating subsequent slices in the product fork, and promote only mature, standalone candidates upstream at a measured cadence rather than opening a PR for every fork commit.
