# Run independent comparisons from the CLI

Use [compare_pocs.py](../../scripts/migration/compare_pocs.py) to launch one new
Codex context per completed case. No conversation is resumed or forked. Runs are
sequential, each with its own workspace, report, event log and timeout. I can run
this same command and return only the reports once the handoffs are ready.

1. Copy [the example manifest](review-batch.example.json) into reviewer storage.
2. After a case meets its acceptance criteria, set its status to `ready` and add:

```json
{
  "name": "geo",
  "status": "ready",
  "starting_sha": "FULL_40_CHARACTER_START_SHA",
  "candidate_identity": "synthetic-base SHA plus complete patch SHA-256",
  "reference_shas": ["FULL_40_CHARACTER_REFERENCE_SHA"],
  "handoff": "geo/handoff.md",
  "completion_evidence": "geo/completion.md"
}
```

Paths are relative to the manifest (absolute paths also work). Fill the handoff
fields in the [comparison prompt](comparison-prompt.md): source and reference
locations, approved requirements/scope, frozen identities, validation results,
resource limits and applicable review guidance. References must be pinned by the
review owner, never inferred from current HEAD. `ready` is an owner attestation;
the reviewer must verify the evidence. Non-blocking findings may remain recorded.

3. Prepare prompts without calling a model:

```sh
python3 scripts/migration/compare_pocs.py /path/to/review-batch.json \
  --output /tmp/poc-review-preview
```

4. Run using a locally available Codex CLI and authenticated account:

```sh
python3 scripts/migration/compare_pocs.py /path/to/review-batch.json \
  --output /tmp/poc-review-run-01 --execute --model MODEL_ID --timeout 3600
```

Choose the intended model explicitly. Output directories must be new; existing
reports are never overwritten. The launcher captures model events in `events.jsonl`
and the final response in `comparison.md`. `execution.json` records the command,
prompt digest, elapsed time and exit status. `summary.json` indexes all cases.
A report being produced does not mean the candidate passed review. Authentication,
missing access, model failures and timeouts remain visible; no automatic retries.

The launcher disables user config loading, uses a fresh scratch working directory
and does not pass this chat or previous verdicts. Global instructions, installed
skills and model training may still influence it: fresh context is not proof of
zero bias. Store scratch outputs outside the migration author tree. For stronger
isolation, use a dedicated reviewer environment with only the approved inputs.

## VM access and limits

Put VM access details in the private handoff, not the shared template. Prefer
pre-provisioned, frozen review inputs and writable scratch copies. To permit the reviewer to inspect the
VM through approved SSM access, add `--remote-access`; AWS CLI credentials and
permissions must already work. This enables network access and is not a read-only
AWS permission boundary. Use restricted reviewer credentials where available;
the prompt prohibits source modification and infrastructure changes. The launcher
does not install credentials, export private sources or start a stopped VM.

Local execution uses the workspace-write sandbox with approvals disabled; a
command requiring escalation fails instead of prompting. Missing permissions
produce an incomplete report. No sandbox bypass is used. Remote commands may
outlive the local CLI timeout: bound their SSM execution time separately and
retain command IDs. The timeout limits local execution, not token spend or remote
compute billing. It stops the local process group and retains partial logs.

Reviewers may test in disposable scratch copies; original candidates and evidence
must remain unchanged. The current three VM cases remain incomplete. Qint needs a
verified handoff for this launcher; an earlier comparison is not that handoff.
No final comparison is started merely by adding its name to the manifest. A dry
run lists ineligible cases as skipped; execution fails before calling a model if
none are ready. The existing qint experiment also has missing workspace/CI
acceptance evidence, despite its earlier diagnostic comparison.

Implementation checked against the installed `codex exec --help` and
[official non-interactive mode documentation](https://developers.openai.com/codex/noninteractive).
The recorded event stream also supports [review usage metrics](metrics.md).
