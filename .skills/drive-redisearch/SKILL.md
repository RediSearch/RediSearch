---
name: drive-redisearch
description: Drive the locally built RediSearch module the way a user does — a real redis-server with redisearch.so loaded (standalone or a 3-shard OSS cluster), exercised through FT.* commands in redis-cli — and capture evidence. Use to prove a change to indexing, querying, aggregation, vector/hybrid search, or the coordinator works end to end, beyond what unit tests show, or to reproduce a reported behavior by hand.
---

# Drive RediSearch like a user

The user-facing surface of RediSearch is the `FT.*` command set, sent to a redis-server that has loaded `redisearch.so`. Users write data with core commands (`HSET`, `JSON.SET`), and the module indexes it through keyspace notifications. Proof means sending those commands to a real server built from this checkout and reading the replies. Calling C functions or using `FT.DEBUG` internals does not count as proof.

This skill complements [/verify](../verify/SKILL.md): that one runs the build, lint and test suites, and this one shows the behavior on a live server. This skill is for driving by hand. For a repeatable regression test, write a flow test instead (see [/write-flow-tests](../write-flow-tests/SKILL.md) and [/run-python-tests](../run-python-tests/SKILL.md)). Use this skill to see behavior, and use the flow test to lock it in.

All instance work goes through `.skills/drive-redisearch/rsv.sh` (reachable as `.claude/skills/drive-redisearch/rsv.sh`, which is the same file through a symlink). Run it from the repo root. It is Linux-only (`ss`, `/proc`, GNU `stat`/`date`), and it finds the build under `bin/linux-<arch>-{debug,release}/`. Each instance has a name (`--name N`, default `default`), its own random ports, and its own data dir. Several instances can run side by side. The script never touches a redis it did not start.

## Launch

1. **Build the module from this checkout.** The server runs whatever `.so` is on disk, so a stale build tests old code.

   ```bash
   export PATH="$HOME/.cargo/bin:$PATH"   # cheadergen lives here; without it CMake fails with "Could not find CHEADERGEN_EXECUTABLE"
   set -o pipefail; LOG=$(mktemp /tmp/rs-build.XXXXXX.log); echo "Log: $LOG"
   ./build.sh DEBUG=1 2>&1 | tee "$LOG" | tail -20
   ```

   Output: `bin/linux-x64-debug/search-community/redisearch.so` (`linux-aarch64-debug` on ARM). The debug build is about 200 MB and has assertions on, which is what you want for verification. If bindgen fails with `unknown type name 'VecSimQuantType'` (or similar errors in `deps/` headers), a submodule is not at its pinned commit. Check `git submodule status`: a leading `+` means it has drifted. Fix it with `git submodule update --init --recursive deps/<name>`.

2. **Pick a redis-server new enough for HEAD.** The module refuses to load on servers that lack the key-metadata module API, and `start` then prints `<search> DocIdMeta requires the Redis key metadata API (Redis 8.6.0 or newer)`. A server built from `redis/redis` `unstable` works. Point the script at it:

   ```bash
   export REDIS_SERVER=/path/to/redis/src/redis-server REDIS_CLI=/path/to/redis/src/redis-cli
   ```

   `rsv.sh` uses `redis-server` / `redis-cli` from `PATH` otherwise. Check the version with `"$REDIS_SERVER" --version`. An unstable build reports `v=255.255.255`, so the version alone does not tell you whether the server is new enough. Starting it and reading the log does.

3. **RedisJSON (only for `ON JSON`).** Search needs RedisJSON API V7 or newer (`RedisJSONAPI_MIN_API_VER` in `src/json.h`). A stale `rejson.so` still loads, but `FT.CREATE ... ON JSON` then fails with `Invalid rule type: JSON`, and doctor reports `ReJSON loaded but search did not acquire its API`. To rebuild RedisJSON master, run the same script the test runner uses (it `git pull`s `tests/deps/RedisJSON` and runs a cargo build, a few minutes):

   ```bash
   PATH="$HOME/.cargo/bin:$PATH" ROOT=$PWD bash -c 'source tests/deps/setup_rejson.sh'
   ```

4. **Start an instance.** `start` waits until the server answers `PING`, then runs `doctor`. `cluster-start` also runs `SEARCH.CLUSTERREFRESH` on every node and waits until each coordinator can fan out. Servers without the cluster-topology-change module event never push the topology to the coordinator on their own, and every `FT.*` command would fail with `ERRCLUSTER Uninitialized cluster state`.

   ```bash
   .skills/drive-redisearch/rsv.sh --name mytest start            # standalone, debug build
   .skills/drive-redisearch/rsv.sh --name mytest start --json     # also load RedisJSON (needed for ON JSON indexes)
   .skills/drive-redisearch/rsv.sh --name mycl cluster-start      # 3-shard OSS cluster, coordinator active
   ```

   The startup options are `--module PATH` (default: the debug `.so`, or the release one if no debug build exists), `--json` / `--json-path PATH`, `--modargs "WORKERS 2 DEFAULT_DIALECT 2"` (module load-time args), and `--shards N` (cluster only, default 3).

   The server is ready when `start` prints `rsv: instance '<name>' up on 127.0.0.1:<port>` and no doctor line says `FAIL`. If startup fails, `start` prints the server's own error lines from `redis.log`. Run `stop` before retrying, because the instance dir is left in place for inspection. RedisJSON comes from `bin/linux-x64-release/RedisJSON/master/rejson.so` (see step 3).

   Instance state lives in `$RSV_ROOT/inst/<name>/` (`RSV_ROOT` defaults to `/tmp/rsv-<uid>`, created mode 700; the script refuses an existing root that is not a mode-700 directory you own, rather than changing its permissions). It holds the per-node `redis.log`, `redis.pid`, and data dir.

## Doctor

Run `rsv.sh --name N doctor` first whenever anything looks off. It exits non-zero exactly when it prints a `FAIL` line. For each node it checks five things: the recorded pid is alive, `INFO server` on the port reports that same pid (so the port belongs to us), `MODULE LIST` shows `name search`, search acquired the RedisJSON API if ReJSON is loaded, and the `.so` has not been rebuilt since the server process started. In a cluster it also checks, on every node, `cluster_state:ok`, that the coordinator can fan out, and that `SEARCH.CLUSTERINFO` reports as many shards as the cluster has. A coordinator that refreshed before gossip converged can hold a smaller topology and quietly return partial results. The fan-out check sends `FT.SEARCH` on an index that does not exist, which is read-only and answers `ERRCLUSTER Uninitialized cluster state` until the coordinator has a topology.

It also prints a `WARN`, which does not change the exit code, when build inputs under `src/` (C, C++, Rust, the `.rl`/`.y` query grammars, `Cargo.toml`/`Cargo.lock`, `CMakeLists.txt`) are newer than the `.so`: you probably need to rebuild. It then prints the module's mtime and the HEAD commit so you can judge staleness yourself. A `FAIL ... rebuilt after this server started` means you are not testing your build. Run `stop`, then `start`.

## Drive

Every command goes through `cli`, which runs `redis-cli --no-raw` against the instance under a timeout (`RSV_CLI_TIMEOUT`, default 120 s). Only `-x` may come before the command. `cli` rejects other redis-cli options, because `-p`, `-h`, `-s` or `-u` would send the command to a server this run did not start. In cluster mode it adds `-c` so key-routed writes follow `MOVED`. `--no-raw` keeps reply types visible (`(error)`, `(integer)`, `(nil)`), because `redis-cli` **exits 0 on an error reply**. Judge success by the reply, never by the exit code. Commands are literal redis-cli argv. Quote query strings once for the shell, and single-quote anything containing `$` (JSON paths, `$param` references).

```bash
R=".skills/drive-redisearch/rsv.sh --name mytest"
$R cli FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT price NUMERIC SORTABLE tags TAG
$R cli HSET doc:1 title "red shoes" price 30 tags sale,new
$R cli FT.SEARCH idx "@title:shoes" RETURN 1 price
$R cli FT.INFO idx
```

To send binary arguments such as vector blobs, use `redis-cli -x`, which appends stdin as the last argument. Put the blob-carrying option last. `printf` builds little-endian FLOAT32 vectors; see [features/vector-search.md](features/vector-search.md).

```bash
printf '\x00\x00\x80\x3f\x00\x00\x00\x00' | $R cli -x HSET vec:1 v
```

Recipes per feature are in [features/README.md](features/README.md). Read the index, then the matching feature file. If the change touches one feature, drive every entry point the feature file lists, not only the most convenient one. That includes both `ON HASH` and `ON JSON` where the file lists both, and standalone and cluster where the file lists both.

## Evidence

Use `rec` to capture proof. It runs the same command as `cli`, prints the reply, and appends a record to `$RSV_ROOT/evidence/<name>/<artifact>.txt`. Each record starts with a stamp line (time, instance, ports, module path and build time, HEAD), then the exact argv, the reply, and the exit code. Records from different runs or builds stay distinguishable even when a name is reused, which matters for before/after-fix comparisons. Error replies are tagged `ERROR REPLY`. For commands that read stdin (`-x`), the bytes sent are recorded as `[stdin hex]`:

```bash
$R rec search FT.SEARCH idx "@title:shoes"
$R evidence-dir      # prints the evidence directory
```

`stop` also copies each node's `redis.log` into the evidence dir as `redis-<port>.log`. Assertion failures and crash stack traces land there, and they are often the proof. Evidence is never deleted by this script. Remove it yourself once the run has been reported.

Proof standards:

- **Exercise the real user path.** Write documents with `HSET` / `JSON.SET` and let the module index them. Read results with `FT.SEARCH` / `FT.AGGREGATE` / `FT.HYBRID`. Do not use `FT.DEBUG` to set state. `FT.DEBUG` and `FT.PROFILE` are fine as *observation* (to show which iterator ran), but they do not replace the user-visible reply.
- **Capture the action and the resulting state.** Record the write, then the query that shows its effect, and for mutations also a second read-only view. For example, after `FT.ALTER`, show both `FT.INFO` attributes and a query on the new field.
- **Check side effects as well as replies.** For deletes, show that `FT.SEARCH` no longer returns the doc *and* `FT.INFO` `num_docs` dropped. For persistence, run `DEBUG RELOAD` (dumps the RDB and reloads it in-process) and re-query. In a cluster, show per-shard state with `"$REDIS_CLI" -p <shard-port>` (ports from `rsv.sh ports`) alongside the coordinator reply.
- **Capture the failure mode too.** For a fix, record the bad behavior on a build without the fix (`--module` pointing at another build, or a second instance name), then the good behavior on yours.
- **Mocks: none.** Nothing here talks to external systems. RedisJSON is a real module, not a mock.

## Cleanup

```bash
.skills/drive-redisearch/rsv.sh --name mytest stop     # one instance (all nodes, if a cluster)
.skills/drive-redisearch/rsv.sh list                   # what is still running under $RSV_ROOT
```

`stop` shuts down only the pids it recorded, and only after confirming each one is still the server on the recorded port. Every call to the server has a timeout, so a wedged server is SIGKILLed after about 5 s instead of hanging `stop`. If a recorded pid is alive but cannot be confirmed as ours, `stop` leaves it running, keeps `$RSV_ROOT/inst/<name>/` so it stays traceable, and exits non-zero. Otherwise it deletes `$RSV_ROOT/inst/<name>/`. Evidence in `$RSV_ROOT/evidence/<name>/` survives. Never run `pkill redis-server`: the user may have their own redis, and the flow tests start their own. Also run `stop` after a failed attempt, so broken runs do not leave ports and processes behind.

## Helpers

`.skills/drive-redisearch/rsv.sh [--name N] <command>`:

| command | does |
|---|---|
| `start [--module P] [--json] [--json-path P] [--modargs "..."]` | standalone server with the module, then runs doctor |
| `cluster-start [same opts] [--shards N]` | N-node OSS cluster (`redis-cli --cluster create`, no replicas), then runs doctor |
| `doctor` | read-only health check, non-zero exit on failure |
| `cli <args...>` | `redis-cli --no-raw -p <first port> [-c] <args...>` |
| `rec <artifact> <args...>` | `cli`, plus append a stamped record (command, stdin bytes, reply, exit code) to the evidence file |
| `port` / `ports` | first node's port / every node's port, for direct `"$REDIS_CLI" -p` use |
| `evidence-dir` | print (and create) the evidence directory |
| `stop` | tear down the instance and keep the evidence |
| `list` | list live instances |

Environment overrides: `RSV_ROOT` (state root), `RSV_NAME` (default name), `RSV_CLI_TIMEOUT` (seconds per `cli`/`rec` call), `REDIS_SERVER` / `REDIS_CLI` (binaries, default from `PATH`). Servers bind to 127.0.0.1 without a password. They start with `--enable-debug-command local`, so `DEBUG RELOAD` works from local clients, and with persistence off (`save ""`, no AOF) except when `DEBUG RELOAD` triggers it explicitly. `MODULE LOAD` stays disabled, so another local user cannot load code into them.

Keeping this skill current: no CI runs these recipes. When you change a feature's behavior, re-run its recipe and update the expected replies in the same PR. When a recipe's reply no longer matches on an unchanged build, fix the recipe.
