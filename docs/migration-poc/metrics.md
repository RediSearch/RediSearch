# Attempt metrics

Use metrics to compare approaches and identify expensive repair loops. Keep them
separate from findings: routine repairs belong in the run log, not the batch report.

```bash
python3 scripts/migration/attempt_metrics.py /path/to/run \
  --output /path/to/run/metrics.json
```

SSM command counts include inspections and background launches; they are not
migration-attempt counts. Saved command durations exclude background work and do
not measure total run time. Status snapshots may be stale, and shell success does
not establish test success. The JSON report retains evidence paths for each
command; keep changing run totals in that report rather than this guide.

The available logs do not establish exact iteration/fix counts, token usage or
model cost. Do not infer these from filenames, test counts or log size. A successful
repair can cover several defects; several attempts can address one defect.

For subsequent runs, record one JSONL entry per completed edit-and-check cycle:

```json
{"case":"geo","iteration_id":"003","kind":"repair","outcome":"passed","evidence":"author/logs/003-validation.log"}
```

Use `kind` values such as `implementation`, `repair`, `optimization`, or
`validation`; use `outcome` values such as `passed`, `failed`, or `interrupted`.
Assign the iteration ID before starting, retain interrupted cycles, and update its
single entry when it finishes. Count a defect as resolved only after its verification
passes; keep a defect ID and evidence in that cycle's log. This ledger is operational
telemetry, not an additional approval gate or a replacement for acceptance evidence.

```bash
python3 scripts/migration/attempt_metrics.py /path/to/run \
  --iteration-ledger /path/to/run/iterations.jsonl \
  --usage-jsonl /path/to/run/agent-events.jsonl \
  --output /path/to/run/metrics.json
```

`--usage-jsonl` is repeatable and reads `turn.completed.usage` from Codex CLI JSONL
logs. Supply each session once; copied session logs would double-count usage.
Cached tokens are already included in input tokens. Missing usage remains unknown;
interrupted turns may incur usage absent from completed-turn records. The script
does not calculate dollars without recorded model, rates and billing evidence.
Comparison sessions must keep their usage separate from migration sessions.

Also retain case/source/candidate identity, model and harness versions, start/end
times, build/test commands and durations, external-decision wait time, acceptance
gaps, and benchmark environment/results. These allow later comparisons of repair
rate, feedback time and cost per accepted case without rewarding skipped validation.
The current extractor aggregates SSM evidence, explicit iterations and token usage;
it does not yet aggregate all of these additional measures.
