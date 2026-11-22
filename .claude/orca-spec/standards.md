# Worker standards — coordinator-curated human feedback only
<!-- folded verbatim into every rendered worker/verifier spec; keep terse; nothing derivable from code/specs/ADRs belongs here -->

- Commit only your own task's files: stage explicit paths and run `git commit -- <paths>` (or check `git diff --cached --stat` first). Other workers share the worktree, so never commit changes you did not make.
