# Vector and hybrid search

A user declares a `VECTOR` field (FLAT, HNSW, or SVS-VAMANA, with a type, dimension and distance metric), stores vectors as binary blobs in hash fields or as JSON arrays, and queries for nearest neighbours. `FT.SEARCH` supports pure KNN, KNN behind a pre-filter, and radius (range) queries. `FT.HYBRID` fuses a text query and a vector query into one ranked list (RRF or LINEAR).

## Sub-features

- `vec-knn`: `"*=>[KNN k @f $q AS dist]"` returns the k closest docs with their distance.
- `vec-filtered-knn`: `"(<filter>)=>[KNN k @f $q]"` searches only among docs that match the filter.
- `vec-range`: `"@f:[VECTOR_RANGE r $q]"` returns all docs within distance r.
- `vec-algos`: `FLAT`, `HNSW`, and `SVS-VAMANA` index types, plus `FLOAT32`/`FLOAT64`/`FLOAT16`/`BFLOAT16` types and metrics `L2`/`IP`/`COSINE`.
- `hybrid-rrf`: `FT.HYBRID idx SEARCH "<text>" VSIM @f $q COMBINE RRF 2 CONSTANT 60`.
- `hybrid-linear`: `COMBINE LINEAR 4 ALPHA a BETA b`, plus `APPLY`/`FILTER` on `@__score`.

## How to get to it (user POV)

- `FT.CREATE ... SCHEMA <f> VECTOR <ALGO> <nargs> TYPE <t> DIM <d> DISTANCE_METRIC <m> ...`.
- `HSET <key> <f> <binary blob>` (little-endian packed floats), or `JSON.SET` with a numeric array on a JSON index.
- `FT.SEARCH ... PARAMS 2 q <blob> DIALECT 2`, and `FT.HYBRID ... PARAMS 2 q <blob>`.

## Driving it with rsv.sh

Preconditions:

- Standalone instance, `FLUSHALL`ed.
- Blobs are built with `printf` and passed through `redis-cli -x`, which appends stdin as the **last** argument. Order every command so that the blob-carrying argument comes last, which usually means `... DIALECT 2 PARAMS 2 q` at the end. FLOAT32 little-endian: `1.0` = `\x00\x00\x80\x3f`, `0.0` = `\x00\x00\x00\x00`.

- **Create.** Run `$R rec vec-seed FT.CREATE vidx ON HASH PREFIX 1 v: SCHEMA name TEXT emb VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2`. Reply `OK`.
- **Write vectors.** Store [1,0], [0,1], [1,1]: run `printf '\x00\x00\x80\x3f\x00\x00\x00\x00' | $R rec vec-seed -x HSET v:1 name "east apple" emb`, `printf '\x00\x00\x00\x00\x00\x00\x80\x3f' | $R rec vec-seed -x HSET v:2 name "north pear" emb`, and `printf '\x00\x00\x80\x3f\x00\x00\x80\x3f' | $R rec vec-seed -x HSET v:3 name "northeast apple" emb`. Each reply is `(integer) 2`.
- **KNN.** Query [1,0]: run `printf '\x00\x00\x80\x3f\x00\x00\x00\x00' | $R rec vec-knn -x FT.SEARCH vidx '*=>[KNN 2 @emb $q AS dist]' SORTBY dist RETURN 2 name dist DIALECT 2 PARAMS 2 q`. The reply is `(integer) 2`: `v:1` with dist `"0"`, then `v:3` with dist `"1"` (L2 reports the squared distance).
- **Filtered KNN.** Query [0,1] among apples only: run `printf '\x00\x00\x00\x00\x00\x00\x80\x3f' | $R rec vec-filtered-knn -x FT.SEARCH vidx '(@name:apple)=>[KNN 1 @emb $q AS dist]' RETURN 1 name DIALECT 2 PARAMS 2 q`. It returns `v:3` "northeast apple", not the closer `v:2` pear.
- **Range.** Run `printf '\x00\x00\x80\x3f\x00\x00\x00\x00' | $R rec vec-range -x FT.SEARCH vidx '@emb:[VECTOR_RANGE 1.5 $q]' NOCONTENT DIALECT 2 PARAMS 2 q`. The reply is `(integer) 2` with `v:1` and `v:3`.
- **Hybrid RRF.** Run `printf '\x00\x00\x00\x00\x00\x00\x80\x3f' | $R rec hybrid-rrf -x FT.HYBRID vidx SEARCH apple VSIM @emb '$q' COMBINE RRF 2 CONSTANT 60 LOAD 3 @__key @name @__score PARAMS 2 q`. The reply map has `total_results` `(integer) 3`. `results` lists, in order, `v:1` (`__score` `"0.032266458496"`) and `v:3` (`"0.0322580645161"`), which match both the text and the vector side, then `v:2` (`"0.016393442623"` = 1/61), which matches only the vector side. RRF scores are sums of 1/(CONSTANT + rank) with 1-based ranks. It also includes `warnings` (empty) and `execution_time`.
- **Hybrid LINEAR.** Run `printf '\x00\x00\x00\x00\x00\x00\x80\x3f' | $R rec hybrid-linear -x FT.HYBRID vidx SEARCH apple VSIM @emb '$q' COMBINE LINEAR 4 ALPHA 0.0 BETA 1.0 LOAD 3 @__key @name @__score PARAMS 2 q`. With `ALPHA 0.0` the text side contributes nothing, so `results` is ordered by vector similarity alone, 1/(1 + L2 distance): `v:2` (`__score` `"1"`), `v:3` (`"0.5"`), `v:1` (`"0.333333333333"`). `total_results` is `(integer) 3`.
- **Hybrid APPLY/FILTER.** Run the same query with `LOAD 2 @__key @__score APPLY '2*@__score' AS doubled FILTER '@doubled>1'` before `PARAMS`. Only `v:2` survives (`doubled` `"2"`), and `total_results` is `(integer) 1`.

## Gotchas

- A blob of the wrong length (for example 4 bytes for DIM 2) is an indexing failure on `HSET` (check `FT.INFO` `hash_indexing_failures`), or a query error on search. It is never silently truncated.
- `$q` inside a single-quoted query reaches the server literally, which is what you want. Inside double quotes the shell expands it to empty. Always single-quote vector queries.
- `FT.HYBRID` returns a map (`total_results`, `results`, `warnings`, `execution_time`), not the `FT.SEARCH` array. Without `LOAD @__key` the rows carry no key name.
- The `FT.HYBRID` `VSIM` argument is `'$q'` (a parameter reference). The blob still goes in `PARAMS`.
- KNN without `SORTBY dist` returns the k nearest in no guaranteed order.
- `SVS-VAMANA` needs a build with SVS support. On a build without it, `FT.CREATE` rejects the algorithm.
