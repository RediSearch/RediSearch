# Tag filter combined with a wide numeric range

A benchmark for queries of the form `@tag:{value} @num:[lo hi]`, where the numeric range
is wide enough to span many leaves of the numeric tree. It measures how the cost of the
same query depends on the order in which documents were inserted, and where that cost
goes.

The dataset is synthetic and seeded. The same documents are indexed twice:

- `idxA`: inserted in an order unrelated to the numeric value. Every leaf of the numeric
  tree then holds documents from the whole document-id range.
- `idxB`: inserted in increasing numeric value. Each leaf then holds a contiguous slice of
  document ids.

## Requirements

- A release build of the module (`./build.sh`), or `MODULE=/path/to/redisearch.so`.
- `redis-server` and `redis-cli` on `PATH`, recent enough to load the module. Use a
  server built without sanitizers: with one, calls from the module into libc go through
  the sanitizer's interceptors and the timings are inflated.
- `python3`, and `bc` for `time_queries.sh`.
- `gdb` and `redis-benchmark` for `profile_queries.sh`.

The scripts start and stop their own server through
[`rsv.sh`](../../.skills/drive-redisearch/rsv.sh), on a random port.

## Usage

```bash
# Server-side time of each query form, mean of 10 runs
sbin/tag_numeric_range_bench/time_queries.sh 10

# Where the time goes for the 24h query: 60 seconds of stack samples per index
sbin/tag_numeric_range_bench/profile_queries.sh 5000 60 profile-out
```

Both read `NUM_DOCS` (documents per index, default 2000000) and `PLANTED` (sizes of the
tag values to query, default `500,2000,5000,20000`) from the environment. Loading two
million documents twice takes a few minutes.

## What the scripts report

`time_queries.sh` prints one line per query with the mean server-side time and the reply:

1. The range written in the query string, for five window widths, on both indexes.
2. The widest window again with the union forced to its flat variant.
3. The same range expressed as `LOAD` + `FILTER`, as `FILTER` on a SORTABLE field, and as
   a single query that counts all five windows.
4. `FT.PROFILE` of the widest window.

`profile_queries.sh` runs the widest window back to back on one connection and samples
the server's stacks with gdb, which works where `perf` is restricted to root. For each
index it prints the share of samples per cost category and per innermost function, and
writes two files to the output directory:

- `stacks_<index>.txt`: the raw samples.
- `folded_<index>.txt`: collapsed stacks, the input format of `flamegraph.pl`.

The categories in `analyze_stacks.py` are matched on module paths of the iterator
implementation. After a rename there, samples move to the "other" category until the
mapping is updated.
