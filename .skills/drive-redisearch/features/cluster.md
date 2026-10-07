# Cluster coordinator

In a Redis OSS cluster, each shard holds part of the keyspace and its own slice of every index. The coordinator, part of the same module, makes that invisible. `FT.CREATE`/`FT.ALTER`/`FT.DROPINDEX` sent to any node reach all shards. `FT.SEARCH`, `FT.AGGREGATE` and `FT.HYBRID` fan out, and the merged reply is what a single node would have answered over the whole data set.

## Sub-features

- `coord-ddl-fanout`: index DDL on one node creates, alters, or drops the index on every shard.
- `coord-search-merge`: `FT.SEARCH` totals, sort order and `LIMIT` paging are global across shards.
- `coord-aggregate-merge`: `FT.AGGREGATE` reducers combine per-shard partials (COUNT, AVG, ...) correctly.
- `coord-hybrid`: `FT.HYBRID` merges text and vector rankings across shards.
- `coord-topology`: the coordinator learns the topology (`SEARCH.CLUSTERREFRESH`, `SEARCH.CLUSTERINFO`) and reports `ERRCLUSTER Uninitialized cluster state` until it does.

## How to get to it (user POV)

- Any `FT.*` command sent to any master node of the cluster. Writes go to the slot owner (`redis-cli -c` follows `MOVED`).
- Per-shard views: `"$REDIS_CLI" -p <shard-port> DBSIZE` shows that shard's slice of the keyspace. `FT.INFO idx` does not: the coordinator fans it out and every node reports the global totals (seen: `num_docs` 8 on each of three nodes holding 2, 2 and 4 keys).

## Driving it with rsv.sh

Preconditions:

- `C=".skills/drive-redisearch/rsv.sh --name <run>"` and `$C cluster-start`. Doctor shows `cluster_state:ok` and `coordinator has topology` on all three ports.
- `$C port` gives the node `cli` talks to, and `$C ports` lists every node. `REDIS_CLI` is set in your shell (SKILL.md *Launch*).

- **DDL fan-out.** Run `$C rec coord-ddl-fanout FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT price NUMERIC SORTABLE tags TAG`. Reply `OK`. Then `for p in $($C ports); do "$REDIS_CLI" -p $p FT._LIST; done` lists `idx` on every shard.
- **Spread data.** Run `for i in $(seq 1 30); do $C cli HSET doc:$i title "item $i shoes" price $i tags t$((i%3)) >/dev/null; done`. Then `for p in $($C ports); do "$REDIS_CLI" -p $p DBSIZE; done` shows every shard holding some of the 30 docs. Record it with `rec`, or append it to the evidence file.
- **Global count.** Run `$C rec coord-search-merge FT.SEARCH idx shoes LIMIT 0 0`. The reply is `(integer) 30`, the sum of the shards.
- **Global sort and page.** Run `$C rec coord-search-merge FT.SEARCH idx shoes SORTBY price DESC LIMIT 0 2 RETURN 1 price`. The reply is `doc:30` (price `30`) then `doc:29`, which are the global top two even though they live on different shards.
- **Merged reducers.** Run `$C rec coord-aggregate-merge FT.AGGREGATE idx "*" GROUPBY 1 @tags REDUCE COUNT 0 AS n SORTBY 2 @tags ASC`. You get three rows `t0`, `t1`, `t2`, each with `n` `"10"`, which is not any single shard's partial count.
- **Topology failure mode.** On a fresh cluster before `SEARCH.CLUSTERREFRESH` (start nodes by hand, or reproduce on a server without the topology-change event), `FT.SEARCH` replies `(error) ERRCLUSTER Uninitialized cluster state, could not perform command`. `cluster-start` performs the refresh for you.

## Gotchas

- `SEARCH.CLUSTERINFO` hung during exploration, when it was issued before the topology was set. Wrap direct calls in `timeout 5`.
- `cli` targets the first node only. For per-shard state use `"$REDIS_CLI" -p <port>` directly, and for writes rely on `-c` (`cli` sets it).
- `FLUSHALL` through `cli` wipes one shard only. Reset the whole cluster with `stop` plus `cluster-start`.
- The cluster has no replicas. Failover and replica-read behavior is not covered by this setup.
- Coordinator-only bugs (merge, paging, reducer combining) do not show on standalone. Drive both when the change is in `src/coord/` or in reducers and sorting. The flow-test equivalent is `./build.sh RUN_PYTEST REDIS_STANDALONE=0 SHARDS=3`.
