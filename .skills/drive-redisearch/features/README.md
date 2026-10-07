# RediSearch driving map

This directory is the maintained source for verifying the user-facing behavior of RediSearch. Read this index before driving the module, then use the matching feature file as the recipe.

## Baseline preconditions

- The module is built from this checkout (`./build.sh DEBUG=1`), and `rsv.sh doctor` prints no `WARN ... sources newer` line for the files you changed.
- `REDIS_SERVER` / `REDIS_CLI` point at a server new enough for HEAD (see SKILL.md *Launch*).
- An instance was started by this run: `R=".skills/drive-redisearch/rsv.sh --name <run>"` and `$R start` (add `--json` for JSON recipes, or use `cluster-start` for cluster recipes).
- `$R doctor` reports no `FAIL`.
- Never drive an instance this run did not start. Leave the user's own redis and the flow-test servers alone.

## Driving conventions

- Recipes start from an empty instance unless their preconditions say otherwise. Run `$R cli FLUSHALL` between recipes. In standalone mode it also drops index definitions. In a cluster, `FLUSHALL` with `cli` reaches one node only, so `stop` and `cluster-start` again instead.
- Setup writes go through `rec` too, so an artifact holds the writes that produced the result next to the read. Recipes whose seed data serves several reads record it in a shared `*-seed` artifact.
- Every command in the recipes is literal `$R cli ...` / `$R rec <artifact> ...` argv. Keep the quoting: double quotes around queries, single quotes around anything with `$`.
- Replies are shown in `redis-cli --no-raw` form. `FT.SEARCH` replies start with `1) (integer) <total>`, followed by key/field pairs.
- Use `RETURN`/`NOCONTENT` to keep replies short and the assertions sharp.

## Proof and skip reporting

- Proof is a `rec` artifact that holds both the write (`HSET`/`JSON.SET`/`FT.CREATE`) and the read that shows its effect, plus a second independent view for mutations (`FT.INFO`, a different query, per-shard state).
- For a fix, put a run on the pre-fix build (`--module <other .so>`) next to a run on yours.
- Copy the server log on `stop`, which happens automatically. Grep it for `ASSERT`, `crashed`, `# ` warnings.
- Record the feature ID (for example `search-tag`) and the entry point (standalone/cluster, HASH/JSON) in the artifact name.
- If an entry point could not be driven (no JSON API, cluster not ready), report it as skipped with the doctor output. Do not report it as verified through another path.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the user-visible behavior. It then uses exactly four H2 sections, in this order:

1. `Sub-features`: short IDs, one line each.
2. `How to get to it (user POV)`: every user entry point.
3. `Driving it with rsv.sh`: starts with `Preconditions:`, then labeled bullets that pair each action with an exact command and its observable result.
4. `Gotchas`: traps that waste or invalidate a run.

## Features

- [Index lifecycle](./index-lifecycle.md): create on HASH and JSON, automatic indexing of writes, alter, info, aliases, drop, delete, indexing failures, and survival across RDB reload.
- [Full-text and field search](./search.md): `FT.SEARCH` text, tag, numeric, prefix and fuzzy queries, sorting, highlighting, the empty result, and syntax errors.
- [Aggregation](./aggregation.md): `FT.AGGREGATE` group-by and reducers, apply and filter, sort, and cursors.
- [Vector and hybrid search](./vector-search.md): KNN, filtered KNN, range queries in `FT.SEARCH`, and `FT.HYBRID` text+vector fusion.
- [Cluster coordinator](./cluster.md): the same commands against a 3-shard OSS cluster, where the coordinator fans out and merges.
