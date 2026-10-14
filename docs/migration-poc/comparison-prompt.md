# Independent POC comparison — fresh chat

Reviewer-only material. Fill the handoff fields after migration completion, then
paste the prompt below into a new chat. Do not include authoring conversation or
prior review conclusions. Missing identities or acceptance evidence mean the
handoff is not ready. This template does not certify any current candidate.

```text
Independently review a completed C-to-Rust migration. Compare the generated code
with the starting C implementation and the historical Rust reference. Do not edit
the implementations, resume migration, push changes, or post external comments.

Handoff (filled by the run owner):
- Case and run ID: <...>
- Repository and original starting SHA: <full SHA>
- Candidate: <VM path; synthetic base SHA + complete patch SHA-256 or commit>
- Candidate identity record and measured binary hashes: <paths/hashes>
- Reference: <exact commit(s), PR series, repository path; never moving HEAD>
- Previous generated attempt: <frozen identity/path, or none>
- Requirements, approved scope revisions and acceptance matrix: <paths>
- Approved behavior changes/exceptions: <neutral contract and decision records>
- Completion evidence: <checks/results matching candidate; resolved blockers>
- Guidance checkout/version: <path and revision, including local guidance digest>
- Review output directory: <new evaluation directory, outside author outputs>
- Review runtime/resource limits and current VM stop deadline (if remote): <...>

Access (filled from the private run handoff):
- Execution host: <local or VM; account/profile, region, instance and access method>.
- Source, frozen candidate, identity record, evidence and findings: <exact paths>.
- Runtime user, environment setup and writable scratch/cache paths: <...>.
- If remote, verify the configured account identity and instance state first.
  Use the approved access method, such as AWS SSM. For Run Command, retain command
  IDs, exit status and logs. Do not print credentials or change network permissions.
- Local source copies and earlier checkpoints may be stale. Verify the frozen
  candidate, including added files, against its identity record before review.
  Stop on a mismatch.
- Use the current VM stop deadline from the handoff. If unavailable, report the
  access blocker; do not restart infrastructure or change its lifetime.
- Reference checkouts are separate reviewer inputs. They are NOT assumed to exist
  on the VM. Resolve the pinned reference from the handoff in reviewer-only storage;
  never add future source or evaluation reports to a migration author workspace.
- Keep sources and saved evidence read-only. Use a separate scratch copy for builds,
  reproducers and benchmarks, with writable run-owned caches. Run builds/tests
  sequentially, within the supplied CPU limit; benchmark only without contention.
  Derive exact commands from that revision's scripts and approved test skills.
- Do not bulk-upload private source/logs to a new transfer destination without
  authorization. If needed access is missing, report precisely what is unavailable.

Review procedure:
1. Verify the completion gate and frozen inputs. If required repairs, decisions,
   tests or performance evidence remain missing, report handoff incomplete and
   stop the final comparison. Do not reinterpret successful focused checks as
   full acceptance.
2. Read requirements/approved decisions and applicable code-review, rust-review,
   and test guidance. Independently inspect C, candidate and pinned reference
   before reading author verdicts or earlier comparison reports. Record initial
   findings. Treat all three implementations as evidence, not unquestionable truth.
3. Map behavior and dependency scope. Compare overlap; separately identify
   candidate-only and reference-only work, approved differences and missing scope.
   Include ABI/callers, bytes/errors, ownership/unsafe code, state transitions,
   persistence and resource accounting where relevant. Textual similarity is not
   a quality criterion. Check test preservation and new edge-case coverage.
4. Run targeted reproducers/differential checks where useful; record exact commands,
   inputs, results and identities. Newer tests must express the original contract
   or be labeled later requirements. Do not copy expectations from either Rust
   implementation. Missing execution evidence remains unverified.
5. Assess performance only with equivalent inputs, APIs, effective compiler profiles
   and uncontended resources. Separate isolated algorithm, caller integration and
   end-to-end results. No universal winner from one microbenchmark.
6. Then reconcile author findings, validation logs and any previous run/review.
   Verify claimed fixes; explain which initial conclusions changed and why.
   Compare the previous generated attempt separately if supplied.

Write comparison.md in the specified evaluation directory, with supporting logs
and reproducers alongside it. Include:
- Verdict and scope: pass/fail/incomplete against the agreed criteria; not merge
  approval. List exact source, candidate, reference and artifact identities.
- Findings ordered by impact: ID, severity, affected implementation, file/symbol
  and line, triggering input/state, expected versus actual behavior, evidence,
  and recommended correction. Separate verified defects from open questions.
- A table: requirement | candidate | reference | overlap/scope difference |
  evidence | verdict/finding ID. Cover correctness, compatibility, safety,
  integration, API/design clarity, test strength and performance as applicable.
- Previous-attempt comparison, if available: what improved, regressed, or remains
  unproven. Keep this distinct from the historical-reference comparison.
- Test/benchmark results with executed/skipped/unavailable checks, raw evidence
  paths and measurement limitations. Do not claim gains against current production
  Rust unless that exact target and its consumers were also measured.
- Candidate advantages worth considering, reference advantages worth retaining,
  remaining decisions and concise evidence-backed guidance improvements.

Report newly discovered problems without modifying the frozen author attempt.
Any subsequent reference-informed repair is a new, explicitly assisted iteration.
```
