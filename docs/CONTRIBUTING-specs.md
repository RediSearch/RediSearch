# Contributing a Spec-Driven Change

For non-trivial changes — new commands, new index types, behavior changes, anything that affects the public API or persistence format — RediSearch uses a lightweight **spec-driven workflow**. Before you write code, you capture the *why*, the *how*, and the *plan* as a small set of review artifacts, so maintainers can agree on the design while it is still cheap to change.

This workflow is **framework-neutral**. What gets reviewed is a set of artifacts (described below), not any particular tool. You can produce them however you like:

- by hand, in plain Markdown — no tooling required;
- with [OpenSpec](https://github.com/Fission-AI/OpenSpec), which this repo ships a setup for under `openspec/` (see [worked examples](../openspec/changes/));
- with [GitHub Spec Kit](https://github.com/github/spec-kit) or another spec framework you prefer.

The artifacts and maintainer review are the contract; the tool that generates them is your choice. You do **not** need to install or run any AI tooling to follow this workflow.

If your change is a small bug fix, a refactor, a test, or a docs update, you can ignore this file and use the standard PR flow described in [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## When you need a spec

Open an issue first if your change involves any of:

- A new `FT.*` command, or a new option/argument on an existing command.
- A new field type or index type.
- A behavior change visible to existing users (different results, different errors, different defaults).
- A cross-module refactor that touches both C and Rust.

For these, a maintainer will either:

1. Reply on the issue with the design decisions and treat the issue as the spec, **or**
2. Ask you (or do it themselves) to draft a spec under `openspec/changes/<your-change-name>/`. This is the path described below.

You do not need to draft a spec to open an issue. The issue comes first; the spec is a follow-up only if the change is large enough to warrant one.

## Where specs live

This repository's convention is the OpenSpec directory layout, which any of the tooling options above (or a hand-written change) can target. The shape is what matters, not the tool that produced it:

```
openspec/
├── config.yaml                       # OpenSpec config (don't usually need to touch)
├── specs/                            # Long-lived specs describing current behavior
└── changes/
    ├── <your-change-name>/           # In-progress change
    │   ├── proposal.md               # Why and what (the pitch)
    │   ├── design.md                 # How (the technical plan)
    │   ├── tasks.md                  # Checklist of implementation steps
    │   └── specs/                    # Delta specs that will land in openspec/specs/ on merge
    └── archive/                      # Completed changes, kept for history
```

If you use a different framework whose on-disk layout differs, that's fine — note it in your PR so a maintainer can help fold the artifacts into this convention on merge.

For shape and tone, browse any subdirectory under [`openspec/changes/`](../openspec/changes/) or [`openspec/changes/archive/`](../openspec/changes/archive/) — each one contains a worked set of the artifacts described below.

## The four artifacts, in plain English

1. **`proposal.md` — the pitch.** Two sections: **Why** (what problem this solves, who is affected, what costs the status quo has) and **What Changes** (the user-visible surface — new commands, changed defaults, removed behavior). Aim for a page, not a dissertation. No code.

2. **`design.md` — the technical plan.** How you intend to implement it: which subsystems change, what the data model looks like, what edge cases exist, what you considered and rejected. This is where reviewers push back on the *approach* before you write the code.

3. **`tasks.md` — the implementation checklist.** Numbered, checkbox-style. Group by subsystem or by phase, so each top-level item maps to one reviewable unit of work. Granularity should be roughly "one task = one PR or one self-contained commit."

4. **`specs/<capability>/spec.md` — delta specs.** The diff against the long-lived specs in `openspec/specs/`. If your change adds a command, you describe the command here; when the change is merged, this content is folded into `openspec/specs/` and removed from `changes/`. For your first PR, you can ask a maintainer to handle this — it's a mechanical step.

## Suggested flow for outside contributors

1. **Open a GitHub issue** describing the problem and what you'd like to change. Reference any prior issues or community discussion. Wait for a maintainer to agree it's worth a spec-driven change.
2. **Draft `proposal.md`** in a fork, under `openspec/changes/<change-name>/proposal.md`. Open a draft PR with just that file. This is the cheapest moment to get directional feedback — before you've written code or a detailed design.
3. **Add `design.md`** to the same draft PR after the proposal is roughly accepted. Iterate on review.
4. **Add `tasks.md`** once the design is settled. From here, you can start implementing; each task can be its own commit or follow-up PR.
5. **Implement.** Update the checkboxes in `tasks.md` as you go. The same PR that contains the implementation should also include the delta specs under `changes/<change-name>/specs/`.
6. **Validate before requesting final review.** A spec-driven change is *done* when all of the following hold. Treat this as a self-check before flipping the PR out of draft, and call it out explicitly in the PR description:
   - Every checkbox in `tasks.md` is either checked off or moved to a follow-up issue with a link.
   - The delta specs under `changes/<change-name>/specs/` describe what actually shipped, not what was originally proposed — update them if the implementation diverged.
   - The new or changed behavior is covered by tests (unit, Rust, and/or Python end-to-end as appropriate). No silent gaps.
   - The full build, lint, and test suites pass locally (see [`CONTRIBUTING.md`](../CONTRIBUTING.md#testing-requirements)) and CI is green.
   - Any user-facing surface — commands, error messages, `FT.INFO` output, configuration — is reflected in the docs, and the PR's release-notes checkbox is set correctly.
   - All review threads from earlier draft rounds are resolved.
7. **Maintainer archives.** On merge, a maintainer moves the change into `openspec/changes/archive/` and folds the deltas into `openspec/specs/`. You don't need to do this step.

A draft PR per artifact is fine; one PR with all four is also fine. Match the size of the change.

## Naming

Use kebab-case for the change directory and keep it short, action-oriented, and descriptive — the name lives in the archive forever, so favour clarity over cleverness. A verb-noun shape (`add-X`, `fix-Y`, `deprecate-Z`) usually reads well.

## What if I don't follow this exactly?

It's fine. The spec workflow is here to make large changes reviewable, not to add procedural overhead. If you have a working implementation and an issue describing it, a maintainer can help you back-fill the artifacts during review. The goal is shared understanding before merge, not paperwork.

## See also

- [`CONTRIBUTING.md`](../CONTRIBUTING.md) — the general contributor guide (setup, coding standards, PR workflow).
- [`openspec/changes/`](../openspec/changes/) and [`openspec/changes/archive/`](../openspec/changes/archive/) — browse for worked examples.
- Spec-framework options (all optional — pick one or write the artifacts by hand):
  - [OpenSpec](https://github.com/Fission-AI/OpenSpec) — the tool this repo's `openspec/` layout matches; its CLI can scaffold the artifacts.
  - [GitHub Spec Kit](https://github.com/github/spec-kit) — spec-driven development toolkit.
