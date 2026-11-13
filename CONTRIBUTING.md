# How we work

## Branches
- `main` is always green: it only changes through reviewed pull requests.
- One branch per part or task: `part-1-ingest`, `part-3-graph-features`, `fix-cycle-join`.

## Commits
- Small and focused, with a message that says what changed and why:
  `Add 24-hour sender features with time-bounded windows`
- Never commit data, model files or credentials (see `.gitignore`).

## Pull requests
1. Open a pull request into `main`, using the template, and link its issue (`Closes #3`).
2. CI must pass.
3. **Review it yourself before merging:** read the diff on GitHub, tick the checklist in the template, and leave a comment on any decision worth remembering.
4. Merge, then delete the branch.

## Issues and milestones
- Each part of the build plan is an issue, assigned to its owner, in a milestone with the target date.
- Bugs and decisions get their own issues, so the history explains why the code looks the way it does.

## Tests
- Every feature function, rule and actor has a ScalaTest spec.
- Prefer tiny hand-made inputs whose right answer can be worked out on paper.

## Releases
- Tag a release at each milestone (`v0.1-ingest`, `v0.4-stage1`, `v1.0-final`), with a short note of what works.
