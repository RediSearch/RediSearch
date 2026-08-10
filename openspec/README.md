# openspec/

This directory holds the **spec-driven change** artifacts for larger RediSearch
contributions. It is the on-disk layout described in
[`../docs/CONTRIBUTING-specs.md`](../docs/CONTRIBUTING-specs.md), which is the
authoritative human-readable guide — start there.

The workflow is **framework-neutral**: the artifacts below (proposal → design →
tasks → spec delta → tests) are what gets reviewed. You can produce them by hand
in Markdown, with [OpenSpec](https://github.com/Fission-AI/OpenSpec) (whose CLI
this layout matches), with [GitHub Spec Kit](https://github.com/github/spec-kit),
or any other spec framework. The tool is optional; the artifacts and maintainer
review are the contract.

## Layout

```
openspec/
├── config.yaml                       # Project context + per-artifact rules
├── specs/                            # Long-lived specs describing current behavior
└── changes/
    ├── <change-name>/                # An in-progress change
    │   ├── proposal.md               # Why + What Changes (the pitch)
    │   ├── design.md                 # How (the technical plan)
    │   ├── tasks.md                  # Implementation checklist (incl. tests)
    │   └── specs/<capability>/spec.md  # Delta specs (fold into ../specs/ on merge)
    └── archive/                      # Completed changes, kept for history
```

## Worked example

[`changes/example-add-ft-foo/`](changes/example-add-ft-foo/) is a small,
**illustrative** change (for a fictional `FT.FOO` command). It is not a real or
planned RediSearch feature — it exists so you can see the shape of all four
artifacts. Copy the directory as a starting point for your own change, or create
the files by hand.

`specs/` and `changes/archive/` start out empty; baseline specs and archived
changes accumulate over time.
