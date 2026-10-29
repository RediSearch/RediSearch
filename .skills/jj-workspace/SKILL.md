---
name: jj-workspace
description: Create or delete a jj (Jujutsu) workspace — a second checkout of this repo, sharing one repository. Use when you need to work on something side by side with the current checkout, for instance to leave a long build or test run undisturbed, and to clean the workspace up afterwards.
---

# jj Workspaces

A jj *workspace* is an additional working copy backed by the same jj repository. Each
workspace has its own working-copy commit (its own `@`), its own `build/` and `bin/`
output, and its own `src/redisearch_rs/target/`. That is the point: two workspaces can
build and test concurrently without contending on the Cargo build-directory lock
(see the "Do not run build/test/lint commands in parallel" rule in `CLAUDE.md` — it
applies *within* a workspace, not across them).

Use one when you need a second checkout **side by side** with the current one. Do not use
one merely to start a new branch: under jj, `jj new` is enough for that.

## Requirements

**jj ≥ 0.46.0 and git ≥ 2.42.0.** jj 0.46 creates each workspace as a real git worktree
(`jj workspace add --colocate`) and removes it again (`jj workspace remove`); everything
below relies on that. Check with `jj --version` before starting. On an older jj, stop and
ask the user to upgrade rather than improvising — the hand-rolled alternative is exactly
what this skill exists to prevent.

## Why the workspace must be a git worktree

jj does not support git submodules — the
[docs](https://docs.jj-vcs.dev/latest/git-compatibility/) say so outright: *"Submodules:
No. They will not show up in the working copy, but they will not be lost either."* This
repository has five (`deps/VectorSimilarity`, `deps/googletest`, `deps/hiredis`,
`deps/libuv`, `deps/snowball`, plus `deps/VectorSimilarity/deps/ScalableVectorSearch`
nested beneath the first), and the build needs them. So each workspace needs a working git
of its own, to run `git submodule` in.

A **git worktree** is the right shape: it has its own HEAD, its own index, and its own
`modules/` tree under `.git/worktrees/<name>/`, so submodules initialised in one workspace
never touch another. `jj workspace add --colocate` creates exactly that.

**The obvious hand-rolled alternative is wrong and damages the rest of the machine.**
Pointing a workspace's `.git` at the shared git *directory* —
`echo "gitdir: …/.git" > .git` — makes it a second checkout sharing one HEAD, one index,
and one `.git/modules/`. Then:

- `git submodule update --init` rewrites `core.worktree` in the *shared* per-submodule
  config to point at whichever workspace ran it last. Every other checkout, including the
  main one, is silently unwired; `git status` there fails with
  `fatal: cannot chdir to '…/deps/VectorSimilarity'`. Deleting the workspace leaves the
  pointers dangling permanently.
- Submodules check out at the **main checkout's** pinned revisions, since the gitlinks are
  read from the shared index.
- `CMakeLists.txt` embeds `git describe` and `git rev-parse HEAD` into the module (the
  `GIT_VERSPEC` / `GIT_SHA` block). Sharing HEAD means every build in a workspace
  reports the **main checkout's commit**. `ERROR_QUIET` hides the failure, so it is wrong
  or blank rather than loud.

Never do that, and never create a workspace with `--no-colocate` (or with `git.colocate`
set to `false` and no flag): the result has no `.git` at all, and `git` run inside it walks
up the tree to whatever repository it finds — typically a `$HOME` dotfiles repo.

Workspaces created by earlier versions of this skill (a staged `git worktree add` moved
into a `jj workspace add` directory) are git worktrees too. jj 0.46 recognises them as
colocated (`jj git colocation status` says so), keeps their HEAD in sync, and removes them
with `jj workspace remove` like any other. Nothing needs migrating.

## Naming and location

Workspaces live in the **parent directory of the repo root**, as siblings of the main
checkout, and are named after it:

- `RediSearch-<feature>` when the workspace exists for a specific task —
  `RediSearch-wildcard-cap`, `RediSearch-mod-16990`. Prefer this; the name says why the
  directory exists.
- `RediSearch-<N>` when there is no single task — `RediSearch-1`, `RediSearch-2`. Pick the
  lowest integer not already taken.

Match the capitalisation of the main checkout (`RediSearch`), and keep the feature suffix
lowercase and kebab-case.

The distinction is not just cosmetic: a `<feature>` workspace you created for a task is
task-scoped and can be cleaned up automatically when that task ends, whereas a `<N>` one is
a general-purpose checkout that stays until the user says otherwise. See
[When to delete a workspace](#when-to-delete-a-workspace).

## Creating a workspace

The snippets in this skill are **bash**, written to run as a script: save one to a temp file
and run `bash <file>`, or feed it through a quoted heredoc (`bash <<'EOF' … EOF`). They use
POSIX syntax (not fish), their `exit 1` would close an interactive shell, and their single
quotes break `bash -c '…'`. Each one
derives its own variables from `$name` rather than relying on an earlier snippet — shell
state does not survive between separate invocations.

### 1. Create the workspace

```bash
repo_root="$(jj workspace root)"
name="RediSearch-<name>"          # see Naming above
ws_path="$(dirname "$repo_root")/$name"

jj workspace list | grep -q "^$name:" && {
    echo "REFUSING: jj already has a workspace named '$name'" >&2; exit 1; }
[ ! -e "$ws_path" ] || {
    echo "REFUSING: $ws_path already exists (leftover from an incomplete cleanup?)" >&2; exit 1; }

jj workspace add --colocate --revision master --sparse-patterns full "$ws_path"
```

jj prints one `ignoring git submodule at "deps/…"` line per submodule here. That is
expected: jj does not check submodules out, which is what step 2 is for.

The two guards check the name is free, both to jj and on disk. A leftover directory needs
a human look before anything reuses its name.

`--colocate` is explicit rather than left to the default. The default colocates only when
the *current* workspace is colocated and `git.colocate` is `true`; running from a checkout
where either does not hold would silently produce a workspace with no git — see above. The
output must include `Created Git worktree for the new workspace.`

`--revision` gives the **parents** of the new working-copy commit, not a revision to check
out:

- `--revision master` — a fresh empty change on top of `master`. The default for work that
  stands alone.
- `--revision @` — a child of the current working-copy commit, deliberately stacked on what
  you are working on now.
- omitted entirely — a **sibling** of the current commit, sharing its parents.

Ask the user if it is not clear which they want; the three give visibly different starting
points. Substitute the revision directly — there is deliberately no `$revision` variable,
because the third option means dropping the flag, which an empty variable cannot express.

`--sparse-patterns full` stays in all three forms. The default is `copy`, which inherits the
current workspace's sparse patterns, so running this from a narrowed workspace yields one
missing sources or `deps/*` — and everything downstream assumes a full checkout.

### 2. Initialise the submodules

```bash
name="RediSearch-<name>"
ws_path="$(jj workspace root --name "$name")" || exit 1
(cd "$ws_path" && git submodule update --init --recursive -- \
    deps/VectorSimilarity deps/googletest deps/hiredis deps/libuv deps/snowball)
```

This takes a while and must succeed. It writes only to this worktree's own `modules/` tree.
`fatal: not a git repository` means step 1 did not create a git worktree; stop and find out
why rather than continuing.

The paths are explicit because a bare `update --init` initialises only the *active* set when
`submodule.active` is configured, and exits 0 having left the rest empty.

### 3. Verify

```bash
name="RediSearch-<name>"
repo_root="$(jj workspace root)"
ws_path="$(jj workspace root --name "$name")" || exit 1
(cd "$ws_path" && jj git colocation status | grep -F "Workspace '$name' is currently colocated with Git.") || {
    echo "NOT READY: workspace is not colocated with git" >&2; exit 1; }
(cd "$ws_path" && jj status)             # the colocation status also prints a `Hint:` line; ignore it
[ -z "$(cd "$ws_path" && jj log -r @ --no-graph -T 'if(empty, "", "x")')" ] || {
    echo "NOT READY: fresh workspace already has working-copy changes" >&2; exit 1; }
subs="$(git -C "$ws_path" submodule status --recursive)" || {
    echo "NOT READY: cannot read submodule status" >&2; exit 1; }
printf '%s\n' "$subs" | grep -E '^[-+U]' &&
    { echo "NOT READY: submodules not cleanly at their pinned commits" >&2; exit 1; }
git -C "$repo_root" status --short          # must not print any fatal:
```

The workspace must be colocated, a freshly created
workspace must have an **empty** working-copy commit, no submodule may report `-`, `+` or
`U`, **and the checkout you started from must still be healthy**. `jj status` reports
changes but exits 0, so the emptiness has to be tested rather than read.

Verify with `submodule status`, not `ls deps/VectorSimilarity`: `ls` succeeds on an empty
directory, so it passes for a submodule that was never initialised. `+` matters as much as
`-`, because configuration such as `submodule.<name>.update=none` makes step 2 exit 0 while
leaving a submodule at its *previous* commit. Checked-out submodules never appear as
untracked in `jj status`, because jj ignores them.

Then build as usual (`/build`); the first build is a full one, since the workspace has no
build cache. It builds the dependency revisions the chosen base pins, which may differ from
what an older checkout has on disk — a failure inside `deps/*` on a fresh `master` workspace
is more likely a dependency/toolchain problem than a botched setup, provided step 3 passed.

## Working in a workspace

Use `jj status` and `jj diff` for the workspace's state, and treat git as present only to
service the submodules and the version stamp. As in any colocated jj checkout, git HEAD is
the working-copy commit's **parent** (`@-`) and is detached; mutating git commands
(`git add`, `git checkout`, `git stash`, `git reset`) fight jj and should not be used.

A workspace's working-copy commit is visible from every other workspace (`jj log` marks it
`@` in its own workspace and shows the workspace name), and `jj` commands run in one
workspace operate on the shared repository. If a workspace's working copy falls behind
after history is rewritten elsewhere, run `jj workspace update-stale` inside it.

## Keeping git state in sync after `@-` moves

jj moves this worktree's HEAD and index to `@-` whenever a command *run in this workspace*
changes its working-copy commit — `jj new`, `jj commit`, `jj edit`, `jj rebase`, and so on.
Two gaps remain, and both are silent:

- **Submodules are never updated by jj.** When the new `@-` pins a different
  `deps/VectorSimilarity` (or any other submodule), the checked-out directory stays at the
  old commit: you build the old one while stamping the new commit. `git submodule status`
  shows it as `+`. Nothing else closes this gap — `make build` does not depend on
  `make fetch`.
- **Rewrites from another workspace leave HEAD stale.** If `@-` is rewritten from a
  different workspace (a `jj rebase` or `jj describe` run elsewhere), this workspace's `@`
  follows but its git HEAD does not: not `jj status`, `jj git export`, `jj git import` or
  `jj workspace update-stale` re-exports it. The version stamp then reports the old commit,
  and the index-derived gitlinks the submodule update reads are the old ones too.

So re-sync whenever `@-` has moved and either of those matters — before any build whose
reported commit or submodule revisions you intend to trust:

```bash
# from inside the workspace
jj new && jj edit @-
git submodule update --init --recursive -- \
    deps/VectorSimilarity deps/googletest deps/hiredis deps/libuv deps/snowball
```

`jj new && jj edit @-` steps onto an empty child and straight back. The two checkouts are
between identical trees, so no file on disk is rewritten (no spurious rebuild), `@` keeps
its change id and contents, the empty child is discarded on the way back, and jj re-exports
HEAD and the index to the current `@-` as a side effect.

**Do not re-sync with `git update-ref HEAD … && git reset`.** jj reads a HEAD it did not
write as an external git change and imports it: it replaces `@` with a new working-copy
commit, and if the old `@` held changes it is left behind as a duplicate sibling.

The submodule line must come *after* the HEAD re-sync, because it reads the pinned
revisions from the index. It is cheap and idempotent, so when in doubt just run both.

## When to delete a workspace

Only ever delete a workspace this skill created. jj itself refuses to remove the workspace
that hosts the repository (whatever it is named), but that is the only thing it refuses.

A workspace created *as part of a task* — the user said "do this in a new workspace", so
its whole reason to exist was that task — may be removed on your own initiative once the
task is done, without asking. Say that you removed it in your final report.

"Done" means the work is durable somewhere else: described and squashed into the stack, or
pushed, or merged. A workspace whose only copy of the work is its own working-copy commit
is not done, whatever the task status says — step 2 below is what checks this, and it
applies even to task-scoped workspaces.

Do **not** auto-remove a workspace when any of these hold — ask the user instead:

- the user created it themselves, or asked for it before the task it ended up serving
- it outlived its task: it has since been used for other work, or holds unrelated changes
- the task did not finish, or finished in a state the user still has to look at
- a build, test run, or process is still using it
- you cannot tell which of the above applies

Asking costs one question; a wrongly deleted workspace costs the user their build cache and
possibly work. When in doubt, ask.

## Deleting a workspace

`jj workspace remove <name>` removes the workspace from jj, removes its git worktree
(including that worktree's submodule metadata under `.git/worktrees/<name>/`), and deletes
the directory with its build output. It is the right tool — but it checks almost nothing
first, so the steps before it are what make it safe.

### 1. Establish the variables

Nothing here may rely on a variable an earlier step happened to leave set. Start from
scratch, and run from a *different* workspace than the one being deleted. Steps 1–4 are one
bash script: paste them together, so that every `exit 1` aborts before the removal.

```bash
name="RediSearch-<name>"          # the workspace to delete, per `jj workspace list`

# Ask jj where that workspace is. Do not set ws_path by hand.
ws_path="$(jj workspace root --name "$name")" || {
    echo "REFUSING: jj does not know a workspace named '$name'" >&2; exit 1; }
[ "$ws_path" != "$(jj workspace root)" ] || {
    echo "REFUSING: run this from another workspace" >&2; exit 1; }
```

Deriving `ws_path` from `$name` makes the checks below inspect the same workspace that
`jj workspace remove` deletes, and proves jj knows the name. `jj workspace root` warns when
the recorded path is unreachable or missing; if it does, stop and have the user confirm
which directory the workspace is.

### 2. Check nothing is lost

```bash
(cd "$ws_path" && jj status && jj log -r '@ | @-') || {
    echo "REFUSING: cannot inspect $ws_path — do not delete what you cannot read" >&2; exit 1; }
[ -z "$(cd "$ws_path" && jj log -r @ --no-graph -T 'if(empty, "", "x")')" ] || {
    echo "REFUSING: $ws_path has an unfinished working-copy commit" >&2
    echo "Show it to the user and get explicit confirmation before deleting." >&2; exit 1; }

# jj ignores deps/*, so the submodules need asking separately
found="$(git -C "$ws_path" submodule foreach --recursive --quiet \
    'git status --porcelain | sed "s|^|$displaypath: |"')" || {
    echo "REFUSING: cannot inspect submodules in $ws_path" >&2; exit 1; }
subs="$(git -C "$ws_path" submodule status --recursive)" || {
    echo "REFUSING: cannot read submodule status in $ws_path" >&2; exit 1; }
found="$found$(printf '%s\n' "$subs" | grep -E '^[-+U]' || true)"
[ -z "$found" ] || {
    printf '%s\n' "$found"
    echo "REFUSING: submodule work would be lost — see above" >&2; exit 1; }
```

**This step is not redundant with `jj workspace remove`.** jj snapshots the working copy
into a commit before removing it, so jj-tracked files survive — an empty one is simply abandoned, a non-empty one is left as an
orphaned commit that
is no longer anyone's `@`, which is easy to lose track of; hence the emptiness check. But
jj ignores submodules entirely, so **uncommitted or unpinned work inside `deps/*` is
deleted without a word**.

If the workspace holds work that is not described, merged, or pushed, tell the user and let
them decide — do not delete on your own initiative.

Three properties of that block are load-bearing, and each exists because the obvious way of
writing it fails silently:

| Written this way | Because otherwise |
|---|---|
| `jj` check `\|\|` exits | a stale or broken `.jj` makes the check *fail*, and an unguarded block reads that as "nothing to lose" |
| `if(empty, …)` test | `jj status` *succeeds* on a workspace full of uncommitted work — it reports, it does not judge |
| `foreach` `\|\|` exits | a corrupt submodule gitdir makes `foreach` exit non-zero with its error on stderr, so `$found` comes back **empty** and the workspace reads as clean |
| `status` captured before filtering | `… \| grep '^+' \|\| true` swallows a failing `status` too, so a corrupt submodule gitdir reads as clean |
| findings collected, then exit 1 | two bare commands only *print*; the warning scrolls past and the deletion proceeds anyway |

The `[-+U]` class matters as much as the abort does. `+` is a submodule sitting on a commit
the superproject does not pin — work committed inside it, which `git status` there reports as
nothing at all. `-` is uninitialised, which `submodule foreach` skips entirely, so any files
under that path are invisible to the first check. `U` is a conflict. All of it lives only in
the per-worktree submodule gitdir, which removal destroys outright.

**Known gap, deliberately left open.** Commit inside a submodule, park the work on a local
branch, then check the submodule back to the pinned commit, and nothing fires: the worktree
is clean and there is no `+`. The obvious catch-all — commits not reachable from a remote —
is unusable here, because the pinned `deps/VectorSimilarity` commit is itself unreachable
from all of its remote refs and the check fires on a pristine workspace. Treat the
submodules as build inputs, not somewhere to work; if you did commit inside one, push it or
copy it out first.

**Never `git submodule deinit`.** It is the remedy the internet suggests for
worktree/submodule trouble and it is actively harmful here: run in a workspace,
`deinit --all` strips the *shared* `submodule.*` config and de-initialises the submodules in
the main checkout and every other worktree. Nothing in this procedure needs it.

### 3. Refuse a locked worktree

```bash
git worktree list --porcelain |
    awk -v p="$ws_path" '/^worktree /{w=(substr($0,10)==p)} w && $1=="locked"{f=1} END{exit f}' || {
    echo "REFUSING: $ws_path is a locked git worktree — ask the user" >&2; exit 1; }
```

jj 0.46 does not honour git worktree locks: `jj workspace remove` deletes a locked
worktree's directory anyway, reports `Removed Git worktree`, and leaves git's admin entry
behind, still marked locked. A lock usually means the directory sits on an unavailable
mount or someone protected it deliberately; unlocking is the user's call, not yours.

The path is taken as `substr($0, 10)` — everything after `worktree ` — not as `$2`, so a
parent directory containing a space still matches.

### 4. Remove it

```bash
jj workspace remove "$name"
```

The output must include both `Removed Git worktree` and `Removed workspace directory`.

Use `remove`, not `forget`. Since jj 0.46, `jj workspace forget` also removes the git
worktree but **leaves the directory** — a multi-gigabyte checkout with no `.git`, which jj no
longer tracks.

**Do not follow up with `git worktree prune`.** It looks like tidy housekeeping and is
global: it drops the admin data of *every* worktree whose directory is missing. Since each
workspace's submodule gitdir lives inside its admin entry, pruning while deleting one
workspace destroys the submodule metadata and refs of any other workspace that happens to be
stale — someone else's unpushed submodule work, gone as a side effect of your cleanup.

### 5. Verify

```bash
jj workspace list
git worktree list
git status --short          # in the current checkout; must not print any fatal:
```

The workspace must be absent from both lists, and the current checkout must still be
healthy.

## Command reference

| Task                              | Command                                                  |
|-----------------------------------|----------------------------------------------------------|
| List workspaces                   | `jj workspace list`                                      |
| Root of a workspace               | `jj workspace root [--name <name>]`                      |
| Create                            | `jj workspace add --colocate --revision <rev> --sparse-patterns full <path>` |
| Check git colocation              | `jj git colocation status`                               |
| Re-sync git HEAD to `@-`          | `jj new && jj edit @-`                                   |
| Refresh after external rewrite    | `jj workspace update-stale`                              |
| Remove (worktree + directory)     | `jj workspace remove <name>`                             |
| List git worktrees                | `git worktree list`                                      |
