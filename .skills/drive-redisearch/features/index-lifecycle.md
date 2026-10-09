# Index lifecycle

A user defines an index over a key prefix with `FT.CREATE`. From then on, every matching `HSET` or `JSON.SET` is indexed automatically, with no explicit add command. The user can inspect an index with `FT.INFO`, change it with `FT.ALTER`, give it aliases, and drop it. Deleting a key removes it from results, and the index definition survives an RDB save and load.

## Sub-features

- `idx-create-hash`: `FT.CREATE ... ON HASH PREFIX ...` indexes existing and new hashes under the prefix.
- `idx-create-json`: `FT.CREATE ... ON JSON` indexes JSONPath fields (`$.a`, `$.tags[*]`) with `AS` aliases.
- `idx-update`: overwriting a field reindexes the doc. A JSON sub-path `JSON.SET` reindexes too.
- `idx-delete`: `DEL` of a key removes it from results and from `num_docs`.
- `idx-failures`: a value that cannot be indexed (for example, text in a NUMERIC field) is counted in `hash_indexing_failures` and not indexed.
- `idx-info`: `FT.INFO` reports definition, attributes, `num_docs`, failures, and indexing progress.
- `idx-alter`: `FT.ALTER ... SCHEMA ADD` adds a field. New writes are queryable on it.
- `idx-alias`: `FT.ALIASADD/ALIASUPDATE/ALIASDEL`. Queries by alias hit the index.
- `idx-list-drop`: `FT._LIST` lists indexes. `FT.DROPINDEX [DD]` drops the index (and with `DD` the docs too).
- `idx-persist`: indexes and their contents survive `DEBUG RELOAD` (RDB dump plus load).

## How to get to it (user POV)

- `FT.CREATE`, `FT.ALTER`, `FT.INFO`, `FT.ALIAS*`, `FT.DROPINDEX`, `FT._LIST` from any client.
- Writes through core commands: `HSET`, `HDEL`, `DEL`, `EXPIRE`, `JSON.SET`, `JSON.DEL`.
- Restart or replica load: exercised locally with `DEBUG RELOAD`.

## Driving it with rsv.sh

Preconditions:

- Standalone instance (`$R start`). Use `$R start --json` for `idx-create-json`, and doctor must show no `FAIL` about the RedisJSON API.
- An empty keyspace (`$R cli FLUSHALL`).

- **Create on HASH.** Run `$R rec idx-create-hash FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT price NUMERIC SORTABLE tags TAG`. Reply `OK`.
- **Index by writing.** Run `$R rec idx-create-hash HSET doc:1 title "red running shoes" price 30 tags sale,new`, `$R rec idx-create-hash HSET doc:2 title "blue shoes" price 80 tags new`, and `$R rec idx-create-hash HSET doc:3 title "red hat" price 15 tags sale`. Each reply is `(integer) 3`.
- **Prove indexed.** Run `$R rec idx-create-hash FT.SEARCH idx "@title:shoes" NOCONTENT`. The reply is `(integer) 2` with `doc:1`, `doc:2`.
- **Info.** Run `$R cli FT.INFO idx | grep -A1 -E '\) (num_docs|hash_indexing_failures|percent_indexed)$'`. The output shows `num_docs` `(integer) 3`, failures `0`, and `percent_indexed` `"1"`.
- **Delete.** Run `$R rec idx-delete DEL doc:3`, then `$R rec idx-delete FT.SEARCH idx hat NOCONTENT`. The reply is `(integer) 0`, and `FT.INFO` `num_docs` drops to `2`.
- **Indexing failure.** Run `$R rec idx-failures FT.CREATE bad ON HASH PREFIX 1 b: SCHEMA n NUMERIC`, then `$R rec idx-failures HSET b:1 n notanumber`. `FT.INFO bad` shows `num_docs` `(integer) 0` and `hash_indexing_failures` `(integer) 1`.
- **Alter.** Run `$R rec idx-alter FT.ALTER idx SCHEMA ADD color TAG`, `$R rec idx-alter HSET doc:4 title x color red`, and `$R rec idx-alter FT.SEARCH idx "@color:{red}" NOCONTENT`. The reply is `(integer) 1`, `doc:4`. `FT.INFO idx` `attributes` lists `color`.
- **Alias.** Run `$R rec idx-alias FT.ALIASADD a1 idx`, then `$R rec idx-alias FT.SEARCH a1 shoes NOCONTENT`. It returns the same docs as querying `idx`.
- **Persistence.** Run `$R cli DEBUG RELOAD` (reply `OK`), then `$R cli FT._LIST` (still lists `idx`) and `$R rec idx-persist FT.SEARCH idx shoes NOCONTENT` (same results as before the reload).
- **Create on JSON.** Run `$R rec idx-create-json FT.CREATE jidx ON JSON PREFIX 1 j: SCHEMA '$.name' AS name TEXT '$.tags[*]' AS tags TAG '$.price' AS price NUMERIC`, then `$R rec idx-create-json JSON.SET j:1 '$' '{"name":"green lamp","tags":["home","sale"],"price":12}'` and `$R rec idx-create-json JSON.SET j:2 '$' '{"name":"desk lamp","tags":["office"],"price":40}'`. Then `$R rec idx-create-json FT.SEARCH jidx "@tags:{sale}" RETURN 1 name` returns `(integer) 1`, `j:1`, `name` `"green lamp"`.
- **JSON sub-path update.** Run `$R rec idx-update JSON.SET j:2 '$.tags' '["office","sale"]'`, then `$R rec idx-update FT.SEARCH jidx "@tags:{sale}" NOCONTENT`. The reply is `(integer) 2`.
- **Drop.** Run `$R cli FT.DROPINDEX idx`, then `$R cli FT.SEARCH idx x`. The reply is `(error) SEARCH_INDEX_NOT_FOUND Index not found: idx`, and `EXISTS doc:1` is still `1` (no `DD`).

## Gotchas

- `FT.CREATE ... ON JSON` failing with `Invalid rule type: JSON` means search did not get the RedisJSON API. It does not mean the syntax is wrong. Check doctor and rebuild `rejson.so` (SKILL.md *Launch*, step 3).
- Indexing existing keys at `FT.CREATE` time runs in the background. On large keyspaces, poll `FT.INFO` `indexing` until it reads `0` before you assert counts. Writes after creation are indexed synchronously.
- A failed field does not make `HSET` fail. The only user-visible signals are `hash_indexing_failures` and the doc's absence from results.
- `FLUSHALL` drops all indexes in standalone. Recreate them before the next recipe.
- `DEBUG RELOAD` needs `--enable-debug-command` (`rsv.sh` sets it to `local`). On a server without it you get `ERR DEBUG command not allowed`.
- `FT.INFO` replies are long. Grep for the key line plus `-A1` rather than eyeballing the output.
