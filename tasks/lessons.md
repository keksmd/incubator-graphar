# Lessons

- Treat newly mentioned workspace files as in-scope task inputs. Do not label an unreviewed untracked file as unrelated work; inspect it before reporting its scope.
- When the user authorizes plan execution and GitHub access, continue with the highest-priority ready slice and create the needed GitHub issues, pull requests, and comments without announcing a proposed next step or waiting for approval.
- Treat GitHub PR checks as required verification: inspect every failed run, use its exact log to fix the root cause, push the repair, and confirm the checks are green before reporting the slice as complete.
- For this repository, follow issue-first delivery: create and cross-link the focused upstream issue before opening or updating any code PR. Keep implementation on its branch until the issue exists; ensure every PR title, body, commit, and discussion comment links the issue and matches the documented plan.
