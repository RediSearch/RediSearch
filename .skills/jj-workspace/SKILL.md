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

## Why this is more than `jj workspace add`

jj does not support git submodules at all — the
[docs](https://docs.jj-vcs.dev/latest/git-compatibility/) say so outright: *"Submodules:
No. They will not show up in the working copy, but they will not be lost either."* This
repository has five (`deps/VectorSimilarity`, `deps/googletest`, `deps/hiredis`,
`deps/libuv`, `deps/snowball`, plus `deps/VectorSimilarity/deps/ScalableVectorSearch`
nested beneath the first), and the build needs them. So the workspace has to be given a
working git of its own, by hand.

**The obvious way of doing that is wrong and damages the rest of the machine.** Pointing
the workspace's `.git` at the shared git *directory* — `echo "gitdir: …/.git" > .git` —
makes it a second checkout sharing one HEAD, one index, and one `.git/modules/`. Then:

- `git submodule update --init` rewrites `core.worktree` in the *shared* per-submodule
  config to point at whichever workspace ran it last. Every other checkout, including the
  main one, is silently unwired; `git status` there fails with
  `fatal: cannot chdir to '…/deps/VectorSimilarity'`. Deleting the workspace leaves the
  pointers dangling permanently.
- Submodules check out at the **main checkout's** pinned revisions, since the gitlinks are
  read from the shared index. A workspace on `master` can silently get a different
  VectorSimilarity than `master` pins.
- `CMakeLists.txt:226-241` embeds `git describe` and `git rev-parse HEAD` into the module.
  Sharing HEAD means every build in a workspace reports the **main checkout's commit**.
  `ERROR_QUIET` hides the failure, so it is wrong or blank rather than loud.

The fix is to register the workspace as a real **git worktree**, which gets its own HEAD,
its own index, and its own `modules/` tree under `.git/worktrees/<name>/`.

Upstream jj is building the same mechanism, as `jj workspace add --colocate` implemented on
top of git worktrees. The durable pointer is the tracking issue,
[jj#8052](https://github.com/jj-vcs/jj/issues/8052); the original prototype
[jj#4588](https://github.com/martinvonz/jj/pull/4588) is closed, having been split into
smaller reviews. It has not landed in any release — jj 0.41.0 has no such flag. **When
`jj workspace add --colocate` ships, delete this machinery and use it.**

To be clear about provenance: **the procedure below is derived here, not taken from
upstream.** Neither the issue nor the prototype mentions submodules or suggests any
workaround. What they share with this skill is the mechanism, and two of the clumsier steps
below exist only because the git CLI cannot do what jj's implementation does natively:

- Step 3 stages the worktree elsewhere and moves its `.git` in, because `git worktree add`
  refuses an existing directory and `jj workspace add` refuses a non-empty one. jj's
  prototype writes its own `git_worktree_add` precisely to escape that — it can create a
  worktree in an existing directory and skip the checkout, which neither `git2` nor `gix`
  offers.
- Step 4's HEAD snapshot goes stale because jj tracks a single `git_head`. The prototype
  migrates to per-workspace `git_heads`, which is what would make the re-sync below
  unnecessary.

Verified against git 2.54.0 and jj 0.41.0. Per-worktree submodule isolation is
version-sensitive — older git may error when `.git/modules/` already holds metadata from a
previous worktree — so re-check these steps if either tool is much older.

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

Five steps. Each line exists because of a failure documented above or noted inline; none of
it is decorative, and the order matters.

### 1. Locate the repository

```bash
repo_root="$(jj workspace root)"
parent="$(dirname "$repo_root")"
name="RediSearch-<name>"          # see Naming above
ws_path="$parent/$name"

jj_git_dir="$(cd "$(jj -R "$repo_root" git root)" && pwd -P)"
git_common="$(git -C "$repo_root" rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)"
[ -n "$git_common" ] && [ "$(cd "$git_common" && pwd -P)" = "$jj_git_dir" ] || {
    echo "REFUSING: git resolves '$git_common', jj says '$jj_git_dir'" >&2; exit 1; }

main_checkout="$(dirname "$jj_git_dir")"
```

Check the name is free first — `jj workspace list` for names jj knows, `ls "$parent"` for
directories an incomplete cleanup left behind. `git worktree` subcommands later must run
against `$main_checkout`, not against whichever workspace you are standing in.

`cd … && pwd -P` rather than `realpath -e`: macOS ships a `realpath` without `-e`, and
`build.sh` supports Darwin, so a contributor there would fail at the first line.

Ask `jj git root` rather than reading `.jj/repo` and `store/git_target` yourself: either can
hold a relative *or* an absolute path, and concatenating an absolute one onto a prefix yields
a path that does not exist. The cross-check against git then guards the opposite failure —
`git rev-parse` walks *up* the tree from a directory that is not a git checkout, so in a jj
workspace with no `.git` yet it cheerfully reports some ancestor's repository, typically a
`$HOME` dotfiles repo. Without it, step 3 registers a worktree in that unrelated repository.
It also catches an empty `git_common`, which would otherwise leave `main_checkout` as `.`.

### 2. Create the jj workspace

```bash
jj workspace add --revision master --sparse-patterns full "$ws_path"
```

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

### 3. Make it a real git worktree

jj refuses to create a workspace in a non-empty directory, so the worktree cannot be made
first. Stage it elsewhere, move its pointer in, repair the recorded path, then point HEAD at
this workspace's own revision:

```bash
test -f "$ws_path/.jj/repo" || {        # step 2 really did create the workspace
    echo "REFUSING: $ws_path is not a jj workspace" >&2; exit 1; }
test ! -e "$ws_path/.git" || {          # never repoint an existing checkout
    echo "REFUSING: $ws_path/.git already exists" >&2; exit 1; }

stage="$(mktemp -d "$parent/.jj-workspace-stage.XXXXXX")"
git -C "$main_checkout" worktree add --no-checkout --detach "$stage/$name"
mv "$stage/$name/.git" "$ws_path/.git"
rm -rf "$stage"
git -C "$main_checkout" worktree repair "$ws_path"

ws_commit="$(cd "$ws_path" && jj log -r @ --no-graph -T 'commit_id')"
git -C "$ws_path" update-ref --no-deref HEAD "$ws_commit"
git -C "$ws_path" reset --mixed -q HEAD

printf '/*\n' > "$ws_path/.jj/.gitignore"
```

Every line answers a specific failure:

| Line | Without it |
|---|---|
| `test -f …/.jj/repo` | step 2 failed on a pre-existing directory and the `mv` attaches a worktree to whatever stale checkout was there |
| `test ! -e …/.git` | the `mv` repoints an already-attached workspace at the staged worktree, corrupting a checkout that worked |
| `mktemp -d` staging | two concurrent creations share one staging dir, and the first `rm -rf` eats the other's staged worktree |
| stage basename `= $name` | git names the admin dir under `.git/worktrees/` after it; a mismatch leaves entries like `-tmp-RediSearch-foo` |
| `worktree repair` | the recorded path still points at the staging directory. Also what makes the `mv` safe under `worktree.useRelativePaths` |
| `update-ref HEAD` | the staged worktree detached at the *main checkout's* HEAD, so submodules check out at the wrong commits, silently |
| `reset --mixed` | `--no-checkout` left the index empty, so git sees every file as deleted and step 4 becomes a no-op |
| `.jj/.gitignore` | `git add -A` stages jj's working-copy state and `git clean -fd` deletes it. jj writes this file itself, but only in the hosting workspace |

Two limits worth knowing. The HEAD is a **snapshot, not a link** — nothing keeps it current,
see [Re-syncing git state](#re-syncing-git-state-after--moves). And the `.gitignore` is not
full protection: `git clean -xfd` removes ignored files by design, so it still takes `.jj/`
with it, in the hosting workspace too. Do not run `-x` cleans in a jj checkout.

### 4. Initialise the submodules

```bash
(cd "$ws_path" && git submodule update --init --recursive -- \
    deps/VectorSimilarity deps/googletest deps/hiredis deps/libuv deps/snowball)
```

This takes a while and must succeed. It is safe now that step 3 gave the workspace its own
`modules/` tree — it writes no shared state. `fatal: not a git repository` means step 3 went
wrong; fix the pointer rather than continuing.

The paths are explicit because a bare `update --init` initialises only the *active* set when
`submodule.active` is configured, and exits 0 having left the rest empty.

### 5. Verify

```bash
(cd "$ws_path" && jj status)
[ -z "$(cd "$ws_path" && jj log -r @ --no-graph -T 'if(empty, "", "x")')" ] || {
    echo "NOT READY: fresh workspace already has working-copy changes" >&2; exit 1; }
subs="$(git -C "$ws_path" submodule status --recursive)" || {
    echo "NOT READY: cannot read submodule status" >&2; exit 1; }
printf '%s\n' "$subs" | grep -E '^[-+U]' &&
    { echo "NOT READY: submodules not cleanly at their pinned commits" >&2; exit 1; }
git -C "$main_checkout" status --short          # must not print any fatal:
```

A freshly created workspace must have an **empty** working-copy commit, no submodule may
report `-`, `+` or `U`, **and the main checkout must still be healthy** — that last check is
what catches a botched step 3 before it costs someone their day. `jj status` reports changes
but exits 0, so the emptiness has to be tested rather than read.

Verify with `submodule status`, not `ls deps/VectorSimilarity`: `ls` succeeds on an empty
directory, so it passes for a submodule that was never initialised. `+` matters as much as
`-`, because configuration such as `submodule.<name>.update=none` makes step 4 exit 0 while
leaving a submodule at its *previous* commit — precisely the wrong-revision build this design
exists to prevent. `jj` printing `ignoring git submodule at "deps/…"` is expected, and is why
checked-out submodules never appear as untracked in `jj status`.

Then build as usual (`/build`); the first build is a full one, since the workspace has no
build cache. If you changed files with `jj` after step 3, re-run its HEAD re-sync first, or
the build stamps the creation commit rather than the code you are testing.

## Working in a workspace

Use `jj status` and `jj diff` for the workspace's state, and treat git as present only to
service the submodules and the version stamp.

`git status` does report this workspace's own files — but it compares them against the
worktree's index and HEAD, which step 4 set once and nothing maintains afterwards. jj does
not update them as `@` moves: it does not know this workspace is colocated, and its own
colocation sync targets the *main* checkout's `.git`. So mutating git commands (`git add`,
`git checkout`, `git stash`, `git reset`) act on an increasingly stale picture.

A workspace's working-copy commit is visible from every other workspace (`jj log` marks it
`@` in its own workspace and shows the workspace name), and `jj` commands run in one
workspace operate on the shared repository. If a workspace's working copy falls behind
after history is rewritten elsewhere, run `jj workspace update-stale` inside it.

## Re-syncing git state after `@` moves

The frozen HEAD is not merely cosmetic. Two things read it, and both go quietly wrong once
the workspace's revision has moved on from where step 4 left it:

- **Submodule revisions.** `make fetch` (`Makefile:247`) is
  `git submodule update --init --recursive`, which reads gitlinks from the frozen index.
  Create a workspace on `master`, rebase two weeks later onto a `master` that bumps
  `deps/VectorSimilarity`, then run `make fetch` because a submodule looks wrong: it checks
  VectorSimilarity back out at the **creation-day** revision. That is the same
  wrong-submodule failure the whole design exists to prevent, just deferred in time rather
  than spread across checkouts.
- **The version stamp.** The `git describe` / `git rev-parse HEAD` in
  `CMakeLists.txt:226-241` also run against the frozen HEAD, so every later build reports
  the creation commit. `ERROR_QUIET` keeps it silent.

So re-run this whenever the workspace's `@` has moved and either of those matters — before
any build whose reported commit you intend to trust:

```bash
# from inside the workspace
ws_commit="$(jj log -r @ --no-graph -T 'commit_id')"
git update-ref --no-deref HEAD "$ws_commit"
git reset --mixed -q HEAD
git submodule update --init --recursive -- \
    deps/VectorSimilarity deps/googletest deps/hiredis deps/libuv deps/snowball
```

The submodule line is part of the re-sync, not an optional extra. `reset --mixed` moves HEAD
and the index but leaves the *checked-out* submodule directories where they were, so if the
new `@` pins a different `deps/VectorSimilarity` you would build the old one while stamping
the new commit — the wrong-revision build this design exists to prevent, arrived at from the
other direction. Nothing else closes that gap: `make build` does not depend on `make fetch`.

It is cheap and idempotent, so when in doubt just run it. Nothing detects a stale HEAD for
you — every one of these failures is silent.

## When to delete a workspace

Only ever delete a workspace this skill created. **Never delete the workspace that hosts
the repository** — see step 2 below, which comes before any destructive action.

A workspace created *as part of a task* — the user said "do this in a new workspace", so
its whole reason to exist was that task — may be removed on your own initiative once the
task is done, without asking. Say that you removed it in your final report.

"Done" means the work is durable somewhere else: described and squashed into the stack, or
pushed, or merged. A workspace whose only copy of the work is its own working-copy commit
is not done, whatever the task status says — step 3 is what checks this, and it applies
even to task-scoped workspaces.

Do **not** auto-remove a workspace when any of these hold — ask the user instead:

- the user created it themselves, or asked for it before the task it ended up serving
- it outlived its task: it has since been used for other work, or holds unrelated changes
- the task did not finish, or finished in a state the user still has to look at
- a build, test run, or process is still using it
- you cannot tell which of the above applies

Asking costs one question; a wrongly deleted workspace costs the user their build cache and
possibly work. When in doubt, ask.

## Deleting a workspace

This path runs `rm -rf` against a directory holding gigabytes of build output and,
potentially, the only copy of someone's work. Every step below either establishes a fact or
refuses; none of it is ceremony. Run it as written rather than from memory.

### 1. Establish the variables

Nothing here may rely on a variable an earlier step happened to leave set. Start from
scratch, and `cd` to the main checkout — never delete the workspace you are standing in:

```bash
repo_root="$(jj workspace root)"
name="RediSearch-<name>"          # the workspace to delete, per `jj workspace list`
jj_git_dir="$(cd "$(jj -R "$repo_root" git root)" && pwd -P)"
main_checkout="$(dirname "$jj_git_dir")"
cd "$main_checkout"

# Ask jj where that workspace is. Do not set ws_path by hand.
ws_path="$(jj workspace root --name "$name")" || {
    echo "REFUSING: jj does not know a workspace named '$name'" >&2; exit 1; }
```

**Deriving `ws_path` from `$name` is what makes the rest safe**, and it replaces three
separate guards. Setting the two independently invites them to disagree — and a `$name` and
`$ws_path` naming *different* workspaces means the final step forgets one while deleting the
other, leaving one untracked and one tracked-but-gone. Asking jj for the path makes that
state unrepresentable, proves jj knows the name (it exits non-zero otherwise), and proves the
workspace belongs to *this* repository rather than some other project that happens to have a
similarly named checkout.

Older workspaces may answer `Workspace has no recorded path`. jj cannot tell you where those
live, so the binding cannot be checked — stop and have the user confirm the name and path
refer to the same workspace before going further.

### 2. Refuse the workspace that hosts the repository

```bash
test -f "$ws_path/.jj/repo" || {
    echo "REFUSING: $ws_path hosts the repository" >&2; exit 1; }
```

In a secondary workspace `.jj/repo` is a *file* holding a path to the real repository; in the
workspace that **hosts** the repository it is a *directory*, so this test fails there — and it
must, because `rm -rf` on the hosting workspace destroys the repository, every other
workspace, and all unpushed work. Its name is no help: `default` can be renamed.

If the user genuinely wants to retire that checkout, stop. Do not `jj workspace forget` it
either — a repository whose hosting workspace has been forgotten is a mess to recover.
Retiring it is a repository-relocation task, not a workspace deletion.

### 3. Check nothing is lost

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

If the workspace holds work that is not described, merged, or pushed, tell the user and let
them decide — do not delete on your own initiative. The `if(empty, …)` test enforces that
rather than trusting you to read the output: an empty working-copy commit has nothing to
lose, a non-empty one might be the only copy of something.

Three properties of that block are load-bearing, and each exists because the obvious way of
writing it fails silently:

| Written this way | Because otherwise |
|---|---|
| `jj` check `\|\|` exits | a stale or broken `.jj` makes the check *fail*, and an unguarded block reads that as "nothing to lose" |
| `if(empty, …)` test | `jj status` *succeeds* on a workspace full of uncommitted work — it reports, it does not judge. Nothing else stops a pasted run from deleting the only reference to that work |
| `foreach` `\|\|` exits | a corrupt submodule gitdir makes `foreach` exit non-zero with its error on stderr, so `$found` comes back **empty** and the workspace reads as clean |
| `status` captured before filtering | `… \| grep '^+' \|\| true` swallows a failing `status` too, so a corrupt submodule gitdir reads as clean |
| findings collected, then exit 1 | two bare commands only *print*; the warning scrolls past and the deletion proceeds anyway |

The `[-+U]` class matters as much as the abort does. `+` is a submodule sitting on a commit
the superproject does not pin — work committed inside it, which `git status` there reports as
nothing at all. `-` is uninitialised, which `submodule foreach` skips entirely, so any files
under that path are invisible to the first check. `U` is a conflict. All of it lives only in
the per-worktree submodule gitdir, which step 4 destroys outright.

**Known gap, deliberately left open.** Commit inside a submodule, park the work on a local
branch, then check the submodule back to the pinned commit, and nothing fires: the worktree
is clean and there is no `+`. The obvious catch-all — commits not reachable from a remote —
is unusable here, because the pinned `deps/VectorSimilarity` commit is itself unreachable
from all 225 of its remote refs and the check reports 39 lines on a pristine workspace. Treat
the submodules as build inputs, not somewhere to work; if you did commit inside one, push it
or copy it out first.

**Never `git submodule deinit`.** It is the remedy the internet suggests for
worktree/submodule trouble and it is actively harmful here: run in a workspace,
`deinit --all` strips the *shared* `submodule.*` config and de-initialises the submodules in
the main checkout and every other worktree. Nothing in this procedure needs it.

### 4. Forget the workspace, then remove it

```bash
# both preconditions come from one record: the worktree must exist and be unlocked
git worktree list --porcelain |
    awk -v p="$ws_path" '/^worktree /{w=(substr($0,10)==p); if(w) seen=1}
                         w && $1=="locked"{locked=1}
                         END{exit seen ? (locked ? 2 : 0) : 1}'
case $? in
    1) echo "REFUSING: $ws_path is not a registered git worktree" >&2; exit 1 ;;
    2) echo "REFUSING: $ws_path is locked — ask the user before unlocking" >&2; exit 1 ;;
esac

jj workspace forget "$name" &&
    git worktree remove --force "$ws_path"
```

The path is taken as `substr($0, 10)` — everything after `worktree ` — not as `$2`. A repo
parent containing a space would otherwise split across fields, the target would read as
unregistered, and the fallback below would leave the admin entry and its submodule metadata
behind.

Both checks precede `jj workspace forget`, and that ordering is the point. `worktree remove`
fails on an unregistered or locked path — if jj has already stopped tracking the workspace by
then, you are left with the directory and the git worktree still present while jj has lost
its `@`. Whether the worktree can be removed is a precondition, not a diagnosis.

A lock usually means the directory sits on an unavailable mount or someone protected it
deliberately; unlocking is the user's call, not yours. An *unregistered* workspace is
recoverable on the spot — it is still a jj workspace, so forget it before deleting the
directory, or its name and working-copy commit stay in `jj workspace list` forever:

```bash
jj workspace forget "$name" && rm -rf "$ws_path"
```

The `&&` is not style. Pasted without `set -e`, a failing `forget` would otherwise fall
through to the removal, leaving the workspace gone but still tracked.

`git worktree remove --force` does the deletion rather than a bare `rm -rf`: it removes the
directory *and* that worktree's `.git/worktrees/<name>/` admin entry in one scoped step.
`--force` is required because plain `remove` refuses a worktree with initialised submodules.
The build output (`build/`, `bin/`, `src/redisearch_rs/target/`) lives there, so this
reclaims several gigabytes.

**Do not follow up with `git worktree prune`.** It looks like tidy housekeeping and is
global: it drops the admin data of *every* worktree whose directory is missing. Since each
workspace's submodule gitdir lives inside its admin entry, pruning while deleting one
workspace destroys the submodule metadata and refs of any other workspace that happens to be
stale — someone else's unpushed submodule work, gone as a side effect of your cleanup. Step 4
already removed the one entry that needed removing.

### 5. Verify

```bash
jj workspace list
git worktree list
git status --short          # in the main checkout; must not print any fatal:
```

The workspace must be absent from both lists, and the main checkout must still be healthy.

## Command reference

| Task                              | Command                                     |
|-----------------------------------|---------------------------------------------|
| List workspaces                   | `jj workspace list`                         |
| Root of the current workspace     | `jj workspace root`                         |
| Create                            | `jj workspace add --revision <rev> <path>`  |
| Detach                            | `jj workspace forget <name>`                |
| Refresh after external rewrite    | `jj workspace update-stale`                 |
| List git worktrees                | `git worktree list`                         |
| Re-record a moved worktree        | `git worktree repair <path>`                |
| Remove one worktree, scoped       | `git worktree remove --force <path>`        |
