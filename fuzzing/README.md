# RediSearch Track B fuzzer

Server-based, **LibAFL-driven**, stateful command-sequence fuzzer for RediSearch.
LibAFL mutates a byte buffer; a structure-aware grammar lowers it into a
valid-but-deep `FT.*` / keyspace command sequence, which is replayed against a
live `redis-server` with `redisearch.so` loaded. Oracles catch crashes, hangs,
and protocol violations.

This is the **Track B** harness described in `FUZZING-TRACK-B.md` at the repo
root. Command execution is only reachable against a live server, and the
module's background threads plus fork-based GC defeat an in-process coverage
forkserver — so this drives commands over the wire and collects a **semantic
state signal out-of-band** to steer LibAFL, rather than instrumenting the
server's code coverage.

## Architecture

- **Grammar as `#[derive(Arbitrary)]` AST** (`ast.rs`). The command grammar is a
  set of enums/structs deriving `arbitrary::Arbitrary`, so a byte buffer decodes
  directly into a structurally-valid sequence — the type shapes *are* the
  grammar, and a byte mutation maps to a local grammar change. References to
  indexes/fields are abstract integers here.
- **Stateful lowering** (`lower.rs`). A live model of created indexes (with field
  kinds and vector dim/type), keys, dicts, and aliases resolves the AST's
  abstract references (`index % live_count`, `field % fields.len()`) so queries
  hit things that actually exist and reach deep execution. Typed query atoms
  carry a `coerce` flag that occasionally resolves against *any* field, so
  type-mismatched queries (e.g. tag syntax on a TEXT field) are still exercised.
- **LibAFL harness** (`harness.rs`). A custom `Executor` lowers each mutated
  input, replays it against the server, and folds a **semantic state
  fingerprint** into a `MaxMapFeedback` map — so LibAFL keeps and further mutates
  any input that reaches a new server state. The fingerprint has three layers, so
  the reward tracks *behavioral/structural* depth rather than only command shape:
  1. **command shape** — verbs reached, sequence length;
  2. **outcome** — per command, OK vs error *class* (hashed) vs timeout, and for
     `FT.SEARCH`/`FT.AGGREGATE` the result-count bucket, so reaching a new error
     path or result regime is rewarded (not just the happy path);
  3. **internal structure** — after each sequence, crash-checked `FT.INFO`
     counters (`num_docs`/`num_terms`/`num_records`/…, power-of-two bucketed to
     absorb GC jitter) plus a forced `FT.DEBUG GC_FORCEINVOKE`, so reaching a new
     index shape — or crashing *during introspection/GC* — is a first-class
     signal.

  Havoc mutators, a minimizer/queue scheduler, and an on-disk corpus come from
  LibAFL; crash/hang classification, signature dedup, and finding artifacts are
  ours.

- **Corpus seeding from pytests** (`seeds/`). The corpus is bootstrapped from a
  committed root set of byte buffers that decode to realistic, deep command
  sequences modeled on the `tests/pytests` workflows (full-text/numeric/tag/geo
  search, vector KNN, GROUPBY aggregation, cursors, JSON, ALTER,
  synonyms/dictionaries, suggestions, debug/GC introspection), so mutation starts
  *outward from valid deep states* instead of first rediscovering that a query
  needs an index. The buffers were generated once, offline: since an input decodes
  via `arbitrary`, an encoder that mirrors `arbitrary`'s byte consumption produced
  a buffer for each pytest-shaped AST and verified it round-trips through `decode`
  before writing it. That generator is not carried in the harness — the committed
  `seeds/` files are the durable artifact, loaded at startup (`--seeds <dir>`).
  Inspect any seed or corpus entry with `--dump-file <path>`.

Grammar coverage: `FT.CREATE` (HASH/JSON, multi-PREFIX, all field types incl.
VECTOR FLAT/HNSW, index options, JSON path fields), `FT.ALTER`, `HSET`,
`JSON.SET`, `DEL`/`FT.DEL`, `FT.SEARCH` (KNN / VECTOR_RANGE, numeric/tag/geo,
affix/fuzzy/wildcard, phrases, extreme LIMIT/DIALECT, most options), `FT.AGGREGATE`
(APPLY/FILTER exprs, GROUPBY + all REDUCEs, WITHCURSOR, LOAD), `FT.CURSOR`,
`FT.DICTADD`, `FT.SYNUPDATE`, `FT.DROPINDEX`, `FT.ALIAS*`, `FT.TAGVALS`,
`FT.EXPLAIN`, `FT.PROFILE`, `FT.SPELLCHECK`, `FT.SUGADD`/`FT.SUGGET`.

## Build

```bash
cargo build --release
```

Standalone crate — not part of the `src/redisearch_rs/` workspace.

## Run

Point it at a built module. Prefer a `SAN=address` build so the crash oracle
catches memory bugs:

```bash
cargo run --release -- \
  --module /path/to/redisearch.so \
  --redis-server /path/to/redis-server \
  --seed 42 --seq-len 48
```

Key flags:

- `--module <path>` — the `redisearch.so` to load (required).
- `--redis-server <path>` — server binary (default `redis-server`).
- `--module-args "<args>"` — module load args (default `"WORKERS 0"`, which
  drops the worker pool to reduce nondeterminism).
- `--seed <u64>` — LibAFL RNG seed, for reproducible campaigns.
- `--seq-len <n>` — max commands per sequence (default 48).
- `--corpus <dir>` — LibAFL working corpus (favored inputs cached during a run).
- `--seeds <dir>` — committed root corpus loaded as initial seeds (default `seeds/`).
- `--rounds <n>` — stop after N fuzzing rounds (each round = one havoc stage =
  many server executions); 0 runs forever.
- `--restart-every`, `--timeout-secs`, `--out-dir <dir>`.
- `--rejson <path>` — load a RedisJSON module too, enabling `ON JSON` indexes
  and `JSON.SET` document ops. A non-ASan RedisJSON loads fine into an ASan
  server; RediSearch's JSON *indexing* path is still ASan-instrumented.
- `--safe-utf8` — suppress raw-binary injection into query text. Use this to
  stop the trivially-triggered UTF-8 decoder over-read (see Findings) from
  masking deeper bugs during a campaign.
- `--malform-rate <n>` — structurally malform ~1-in-`n` rendered commands (bad
  count-prefixes, dropped/duplicated/truncated args, wrong-type values, garbage
  injection) to stress the argument parsers (default 6; 0 disables). Byte-driven,
  so findings still replay.
- `--no-swarm` — disable the config swarm (below).
- `--no-auto-reduce`, `--no-structure-feedback`.
- `--verbose` — print every command as executed with its outcome.
- `--preload <path>`, `--asan-options <str>` — for loading an ASan module into a
  non-ASan server; on this macOS the ASan server is built directly instead.

**Config swarm.** By default each server (re)start runs under a randomized
`search-*` config regime (worker pool 0/2/4 → threaded mode, GC policy/timing,
numeric/vector encodings, dialect), applied via `CONFIG SET` after boot. The same
grammar then exercises wholly different code (the threaded result-processor path,
alternate index encodings, aggressive GC). The regime is recorded in each finding
and re-applied on replay, so reproducibility survives the added nondeterminism.

**Structural malformation.** On top of the value-level fuzzing in queries, the
`--malform-rate` pass corrupts the command *envelope* — the layer the argument
parsers see — so count-prefixes disagree with their payloads, keywords duplicate,
required args go missing, etc. Kept at a modest default rate so deep state still
builds.

On macOS 26 the module and `redis-server` must both be built with Homebrew
`llvm` 22 (not `llvm@21`, whose ASan runtime hangs at startup).

The terminal shows LibAFL's monitor line (execs, exec/sec, corpus, objectives,
state-map coverage) plus our `[FINDING #n … sig=[…]]` lines. Findings are
deduplicated at runtime by crash signature (the ASan report kind plus the first
faulting RediSearch/deps source frame, skipping Redis's own crash-handler
frames); each distinct signature is saved and reported once.

## Findings are self-contained Python repros

Each finding is written as a **runnable Python reproduction script**
(`findings/crash-N.py`) — no separate replay/minimize tooling. The script is
already auto-minimized (see below), spawns its own `redis-server` with the module
loaded, applies the recorded config regime, replays the sequence, and reports
whether the server crashed. To reproduce or share a finding, just run it:

```bash
python3 findings/crash-1.py --redis-server ./redis-server --module ./redisearch.so [--asan]
```

It needs only `pip install redis`. The script header records the crash
signature, the salient server-log lines, and whether the sequence reproduces
standalone; the full server log is saved alongside as `crash-N.log`.

**Auto-minimization + validation.** On the first sighting of each distinct crash
signature the fuzzer delta-debugs the sequence (command-level ddmin + intra-
command shrink), *preserving the exact signature*, then validates the result on a
fresh server. Reducible bugs shrink to a couple of commands; a bug that needs
accumulated state keeps the sequence it actually needs and is flagged
`Standalone: NO` in the script header.

`--dump --seed N` prints the command sequence a seed decodes to without a server,
for inspecting the grammar. `--dump-file <path>` decodes and prints any committed
seed or corpus entry the same way.

## Oracles

- **crash / sanitizer** — server process death or an ASan/UBSan report.
- **never-hang** — no reply within `--timeout-secs`, or `PING` failing after a
  sequence while the process is still alive.
- **well-formed reply** — RESP protocol violations. A normal `-ERR` reply to a
  bad command is *not* a finding.
