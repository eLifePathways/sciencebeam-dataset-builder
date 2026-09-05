# Instructions for AI agents working in this repo

When proposing an implementation plan (before or during `ExitPlanMode`),
always run a second, critical review pass over the plan before presenting
it: look for unspecified edge cases, tie-breaks, fragile assumptions,
scope creep into already-shipped code, and places where wording glosses
over a real decision. Fix what the review finds, then present the revised
plan.

## Prose conventions

Apply these to everything you write - specs, notes, commit messages, and
chat:

- **Avoid "fold ... in / into / together"** as a synonym for add / merge /
  incorporate - use those plain verbs instead. Genuine k-fold / CV / bootstrap
  folds are fine.
- **Notes describe current state only** - minimal and forward-looking, no
  history or migration narration; git history is the changelog.

## Implementing a spec

- Read the named `.project-notes/specs/<name>.md` in full first -- it is the
  source of truth for the goal and scope; stay inside its "What NOT to do" and
  "Design questions" boundaries.
- Set up the worktree per `.project-notes/worktree-setup.md` (`git new-worktree
  <branch>` off `main`, never chained off another feature branch).
- A spec is `done` only once merged into `main`. This repo squash-merges, so the
  branch's commits never become ancestors of `main` and `git merge-base
  --is-ancestor` reports false for work that is fully merged. Compare content
  instead: `git diff <branch-tip> main` empty means `main` has exactly the
  branch's work. Then set `status: done` and update the roadmap.
- After merging, remove the worktree with `git remove-worktree <path> --force`
  rather than `rm -rf`, so the shared symlinks are unlinked without touching
  what they point at. Note this deletes anything gitignored inside it, `output/`
  included.
