# Tasks: FT.FOO (example)

> Illustrative example — `FT.FOO` is fictional. Each top-level task is roughly
> one reviewable commit or PR. Note the explicit test tasks: a change is not
> done until new behavior is covered and the suites are green.

## 1. Command registration

- [ ] 1.1 Register `FT.FOO` in `src/module.c` with read-only command flags
- [ ] 1.2 Wire the handler to the shared index-spec lookup used by `FT.INFO`
- [ ] 1.3 Return `Unknown index name (or name is an alias itself)` on a miss

## 2. Summary formatting

- [ ] 2.1 Add a helper that formats `index=<name> docs=<n> fields=<m>`
- [ ] 2.2 Read counts from the resolved spec (reuse `FT.INFO`'s `num_docs`)

## 3. Tests

- [ ] 3.1 C unit test for the formatting helper
- [ ] 3.2 Python end-to-end test: summary on a populated index
- [ ] 3.3 Python end-to-end test: empty index (`docs=0`)
- [ ] 3.4 Python end-to-end test: unknown index returns the standard error
- [ ] 3.5 Python end-to-end test: alias resolves like other commands

## 4. Docs & spec delta

- [ ] 4.1 Update the delta spec under `specs/foo-command/spec.md` to match what shipped
- [ ] 4.2 Add user-facing command documentation
- [ ] 4.3 Confirm the PR's release-notes checkbox reflects the new command
