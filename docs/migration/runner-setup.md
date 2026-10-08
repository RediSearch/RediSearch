# Migration runner setup and common layout

Source snapshot preparation is implemented by the script below. The remaining
sandbox, access, budget, and artifact-freezing controls are a runner contract,
not an implemented runner. Verify them before claiming enforced isolation.

## Shared guidance and case arguments

The runner supplies the [prompt](prompt.txt), a filled [run manifest](run-template.md),
and pinned skills. The README is for humans and need not enter agent context.
Common instructions stay in these files; each case supplies only its arguments,
requirements, exceptions, and concrete validation plan.

```text
.skills/                         Reusable migration/readiness/feedback guidance
docs/migration/                  Shared prompt, run template, runner setup
docs/migration-poc/              Human POC planning and post-run evaluation guide
cases/<case>/                    Author-visible inputs
  manifest.md                    Identity, source/guidance revisions, environment, limits
  task.md                        Objective, requirements, scope hint, focus, exclusions
  validation.md                  Exact commands, checkpoints and acceptance criteria
runs/<run-id>/                   Outputs, in runner-managed artifact storage
  author/                        Scope revisions, candidate, checks, findings, usage
  evaluation/                    Separate final-review output, written after handoff
evaluation-cases/<case>/         Private reference inputs, outside author storage
```

These are logical locations, not a requirement to commit generated artifacts or
private references to the repository. Record actual paths in the manifest. Mount
only author inputs and writable author outputs during migration; do not expose
the entire guidance checkout if it contains POC answers. Preserve relative links
within the exported, reviewed skill bundle.

Each task needs repository, full starting SHA, and target component/functionality.
The objective is the shared behavior-preserving quality standard. Scope hints,
focus areas, exclusions, and other constraints are optional. Trusted setup resolves
prerequisite/submodule pins; the runner supplies its configured execution profile
(model, access/environment, budgets, guidance, paths). The agent derives requirements,
scope proposals, tests, and benchmarks during readiness rather than asking humans
to prefill these. Each run records its resolved configuration and approved overrides. Resolve conflicts explicitly;
case instructions must not silently relax requirements or access restrictions.

## Prepare the source with the script

Run [prepare_snapshot.py](../../scripts/migration/prepare_snapshot.py) from trusted
setup, outside the migration sandbox. It needs Python 3.11+ and Git. The input is
an existing local checkout; clone/fetch and dependency acquisition belong to trusted
setup, never the migration agent. All pinned submodule commits must be available
in initialized local submodule checkouts. The script does not fetch or execute build
commands and does not copy dirty/untracked files.

From the guidance repository root:

```sh
python3 scripts/migration/prepare_snapshot.py \
  --repo /path/to/trusted/RediSearch \
  --sha <full-starting-commit-sha> \
  --output /path/to/new-run-input
```

The output parent must exist and the output directory must not exist. The script
exports raw tracked blobs, recursively expands pinned submodules, preserves modes
and internal symlinks, then creates `source/` with a fresh one-commit Git repository
and no remote. It does not retain the original object database, branches, tags,
history, hooks, or Git configuration. Git attributes cannot run filters or omit
tracked files during export. `provenance.json` records the original SHA, submodule
pins, content digest, synthetic baseline commit, and preparation-script digest.
Identical inputs produce the same snapshot identity under the same Git setup.

Missing pinned objects, external symlinks, and LFS pointers fail explicitly; existing
output is never overwritten. LFS materialization is not implemented: such a case
requires a separately reviewed trusted preparation step and recorded provenance.
On failure, discard the attempt; the script removes only the output it created.
Source-side prerequisites remain unchanged.

After preparation:

1. Record provenance in the run manifest. Validate build-required metadata and
   baseline commands; the fresh Git commit intentionally differs from the source SHA.
2. Inspect the source and approved guidance for target-specific future answers.
   The script exports the pinned tree; it cannot determine whether its content leaks
   a solution. Keep newer POC planning/reference documents out of the author bundle.
3. Supply the snapshot and reviewed guidance to the restricted runner. Do not mount
   the trusted checkout, credentials, or evaluator storage. Enforce network/tool
   restrictions separately and test them from inside the agent environment.
4. Configure aggregate limits and output collection, then launch the migration.

This script isolates Git contents; it does not sandbox the process that uses them.
Its fixture checks can be rerun with:

```sh
python3 -m unittest discover -s scripts/migration -v
```

## Writable caches before launch

Trusted setup creates a run-owned writable `CARGO_HOME` and build/output directories,
then provisions the approved dependency revisions and toolchains. Record their paths
and dependency identities in the manifest. Verify writes and a focused offline build
as the actual worker user inside the runner before starting migration.

Do not require write access to the developer's shared cache. Seed only approved
cache contents; historical runs must not expose future Git objects through dependency
caches. A writable cache does not establish a historical dependency pin: obtain it
from a lockfile or build record, or explicitly record an approved reconstructed baseline.
Routine cache setup failures belong to `environment-setup`; repair and retry without
human escalation unless access or an unresolved dependency decision prevents it.

## Starting-SHA isolation

A trusted setup process outside the agent environment prepares the source:

1. Resolve the starting SHA and pinned submodules/dependencies. Export tracked
   source, including required submodule contents and materialized LFS assets where
   applicable. Preserve executable bits/symlinks, and validate required build inputs.
2. For the initial pilot, initialize fresh local Git metadata over the export.
   Record the original SHA/submodule mapping, export digest, and new local baseline
   commit. A synthetic snapshot commit is not the original repository SHA.
   Build scripts that need version/history metadata require a documented setup
   adaptation; do not make up history or mount the full original repository.
3. Supply only allowed source references at the starting SHA or its permitted
   ancestors. General skills may be newer, but pin and inspect them to exclude
   target-specific future implementations, fixes, or review answers.
4. Mount no developer checkout, original `.git`, shared object store/alternates,
   future refs, private evaluation inputs, or earlier evaluation reports. Remove
   repository credentials/connectors from the agent environment.
5. Deny arbitrary network access. Preinstall approved toolchains/dependencies or
   provide controlled downloads. Run build scripts with the same restrictions;
   access through shell tools or a subagent must not bypass the boundary.
6. Verify restrictions from inside the actual agent environment before launch.
   Record what was checked and any gap. A detached checkout or missing Git remote
   alone does not isolate future commits. Folder names and prompt rules do not
   enforce access restrictions.

Allow local source inspection, builds, tests, diffs, and candidate commits. If history
is needed later, provide a separately verified repository containing only the start
commit and its ancestors, with no other refs, dangling future objects, or alternates.
Rebuild/isolate that package outside the author environment.

A snapshot may already contain vendored code or recorded metadata. Inventory inputs
and pin them rather than claiming absence of all external knowledge. This setup
controls supplied/retrievable repository information, not a model's prior training.
Report accidental exposure and mark the affected attempt as not isolated.

## Execution, handoff, and review

The runner enforces aggregate spending/runtime/retry limits across author and optional
automated reviewers/subagents. Preserve counters on resume. Skills report usage but
cannot themselves provide enforcement. All migration workers share the source/access
boundary and use separate build outputs when running concurrently.

The migration performs readiness, scope proposal, implementation, validation, and
bounded repair. Automated review can support those steps without a target solution.
At completion or a limit, save candidate identity, evidence, and status (ready for
review or incomplete). Freeze `author/` artifacts before final review. A waiting
batch decision can resume the same active attempt with preserved counters; final
post-run review must not silently reopen an ended attempt.

Final review is manual initially. It is separate from migration and does not imply
merge approval merely because the agent finished. Only selected historical POCs
use the [reference-comparison guide](../migration-poc/evaluation-guide.md). Reviewer
inputs stay read-only; reports go to `evaluation/`. The author never writes there.
Any subsequent migration attempt receives a new run ID and links to its predecessor.
If reference findings are supplied, mark it assisted and explicitly record the
knowledge-boundary exception; do not present it as a starting-SHA-only replay.

## Pilot checks before launch

- Confirm exported file/submodule identity and run baseline build/test commands.
- Verify run-owned cache/build/output writes and an offline dependency smoke build
  as the worker user; resolve dependency pins before the author needs them.
- Verify that future Git objects, other checkouts, network endpoints, repository
  connectors, and evaluator inputs are unavailable to every migration worker.
- Exercise a small budget/timeout stop and resume; confirm counters and logs persist.
- Verify an ended candidate remains unchanged while manual review writes its report.
- Record unavailable controls honestly; choose supervised execution or fix them
  before claiming autonomous enforcement.
