# Lessons from assisted attempts — 12 October 2026

These observations improve guidance; they do not establish POC acceptance or
prove that a fresh autonomous runner follows it. Keep case-specific lessons out
of historical author inputs. The original blind attempt must remain reproducible.

| Observation | Guidance changed | Check on a later run |
| --- | --- | --- |
| GEO's broad non-finite guard rejected defined radius behavior as well as undefined coordinate conversion | Compatibility: classify per operation | Preserve defined radius results; isolate any approved coordinate rejection |
| Runtime compression changes and GC/repopulation exposed construction-time assumptions | Compatibility: test state transitions and surviving C callers | Change configuration between writes; GC an empty index and repopulate |
| Existing Rust reported 40 bytes for an 80-byte boxed payload | Compatibility/validation: independent allocation invariants | Detect undercounting without copying the implementation's reported total |
| Unit-test builds selected `optimised_test` despite a release-named output directory | Validation: record effective profiles and binary hashes | Reject mismatched performance comparisons; rerun matching builds |
| Tracked `.install` files were omitted during transport | Runner setup: verify destination contents before author launch | Detect a missing tracked dot-directory even when Git HEAD matches |
| Baseline coordinator failures and skipped environments could obscure acceptance | Validation: distinguish origin, execution counts and pass criteria | Report baseline failures without claiming a green gate |

Evidence is retained outside the guidance repository:

- Local checkpoint: `.migration-runs/poc-batch-20261012/REMOTE-STATUS.md` under
  `/w/tree/RediSearchEnterprise-re-tests`; repair scripts and SSM results are in
  that run's `transfer/aws/` directory.
- VM root: `/opt/rust-migration/poc/<case>/author/remote/`.
  GEO: `radius-compatibility.log`, `flows-radius-final.log`.
  Inverted index: `memory-accounting-probe.log`, `flows-behavior-fixed.log`.
  Varint: `benchmark/paired-results.json` and retained invalid-profile results.
  Transport: `source-restoration.json` for each case.

On 14 October, the user deferred the Rust-only accounting correction and accepted
varint's existing unsafe terminated-buffer precondition. The guard-page probe
crashes in both original C and candidate Rust; public-command reachability is not
established. These decisions resolve scope questions, not failing-check evidence.

Geo's coordinator failures came from an unrelated localhost Redis listener entering
an unmocked TLS configuration path. Existing CTest suites passed in a private
network namespace: 714 candidate tests and 712 C-baseline tests, with the same two
disabled tests. Direct binary execution had also omitted fixture setup and process
isolation. Validation guidance now preserves the canonical runner's assumptions.
Evidence: `/opt/rust-migration/poc/resume-20261014/geo-ctest-{candidate,baseline}.log`
and `varint-reader-safety.json` on the VM.

RedisJSON was reconstructed from immutable commits before each starting SHA and
built with its checked-in lockfile. Loading the module alone did not enable JSON
tests: the historical runner also requires `REJSON=1`. Count executed tests and
skips explicitly; an exit code of zero did not establish JSON coverage.

Varint's codec microbenchmark gains did not carry through to every application
workload. Two matched module comparisons measured phrase-query median wall time
about 2.3% and 2.0% above C, while a same-binary control was near parity. This
supports the existing requirement for caller-level performance checks; it does
not establish that FFI is the cause or justify accepting a regression. Evidence:
`resume-20261014/varint-module-{benchmark,aa-control,replication}.json` on the VM.

Expanding varint's boundary to include offset iterator callbacks improved the
phrase and highlight workloads against the prior generated module. The current
candidate passed 358 C++ tests and 48 executed flow tests (four skipped). It has
not established full performance acceptance against C. A separate writer
optimization showed no benefit and was reverted. Preserve failed experiments
and benchmark identities; do not carry evidence from an older binary forward.
Evidence: `resume-20261014/varint-byte-offset-{benchmark,vs-prior}.json`.

C deliberately retains one empty inverted-index block because its writer assumes
a last block exists. Rust allocates on the next write. This is an allocation
policy change, not evidence of a legacy bug. Eight C/Rust × HASH/JSON × RESP2/3
cases passed GC, direct repopulation and same-build RDB reload checks. C also
reports zero blocks after empty reload. Keep observable statistics changes
explicit; the separate accounting defect prevents treating reported memory
reductions as measured allocation savings. Existing statistics assertions remain
unchanged. Evidence: `resume-20261014/index-empty-lifecycle.json`.

The three VM candidates still have acceptance gaps. These are supported lessons from repairs
and failed checks, not a completed independent review of their final code.

Promote demonstrated, reusable lessons into the relevant skill reference. Keep
case-specific evidence here, and have the prompt point to the rule rather than
repeat it. Do not put future reference-PR solutions into historical author inputs.
The validation rules above live in [validation guidance](../../.skills/port-c-module/references/validation.md);
allocation-policy differences are covered by the [compatibility checklist](../../.skills/port-c-module/references/compatibility.md).

After each completed POC, independently compare the frozen candidate using the
[review prompt](comparison-prompt.md). Record the evidence supporting each guidance
change and test it in a new migration run that exercises the same risk, without
exposing future implementation details. Passing document or schema checks alone
does not show that agent decisions improved.
