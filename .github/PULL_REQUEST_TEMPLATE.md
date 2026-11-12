<!--
Keep this description skimmable — a reviewer should get the point in ~30 seconds.

- Title: `[MOD-xyz] concise user-facing summary`
- Write for someone who has not read the diff and will not read all of it
- Describe outcomes and behavior, not an implementation play-by-play — the diff
  is authoritative for *how*; this body owns *what* and *why*
- Link the ticket, design doc, or discussion instead of restating background
- Keep every section, including ones that do not apply — write "N/A" instead of
  deleting them
-->

## Describe the changes in the pull request

<!--
1-3 sentences each. No file-by-file walkthrough, no restating what the code does.

- Current: the behavior or limitation today, and why it is worth changing now
- Change: what is different, in user- or API-visible terms
- Outcome: what a user, operator, or caller can observe that they could not before

For internal-only work (refactor, CI, test, dependency), say so plainly in
"Outcome" and state what it enables or unblocks instead of inventing user impact.
-->

1. Current:
2. Change:
3. Outcome:

#### Which additional issues this PR fixes

<!-- Ticket and issue links only. Write "N/A" if there are none. -->

1. MOD-...
2. #...

#### Main objects this PR modified

<!--
3-5 entries. The subsystems, commands, or types a reviewer should look at first,
each with a few words on what changed there. Not a file list — `git diff --stat`
already says which files moved, and it says it more accurately.
-->

1. ...

#### Mark if applicable

<!--
Tick these on the observable surface, not the intent: a changed reply shape,
error string, or default is an API change even when the code change is small.
If either box is ticked, the description above must say what breaks, what the
compatibility story is, and whether a migration or version gate is needed.
-->

- [ ] This PR introduces API changes
- [ ] This PR introduces serialization changes

#### Release Notes

<!--
Exactly one box — CI fails on zero or two. "Requires" for anything a user can
observe: new commands or options, behavior changes, bug fixes, performance
changes. "Does not require" for internal-only work.
-->

- [ ] This PR requires release notes
- [ ] This PR does not require release notes

If a release note is required (bug fix / new feature / enhancement), describe the **user impact** of this PR in the title.
