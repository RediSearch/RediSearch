# Aggregation

`FT.AGGREGATE` runs a query and then pipes the matches through a pipeline of steps: `LOAD`, `GROUPBY` with reducers, `APPLY` expressions, `FILTER`, `SORTBY` and `LIMIT`. It returns rows of computed fields rather than documents. `WITHCURSOR` lets a client page through large results.

## Sub-features

- `agg-groupby`: `GROUPBY n @f... REDUCE <fn> nargs args AS name` (COUNT, SUM, AVG, MIN, MAX, COUNT_DISTINCT, TOLIST, FIRST_VALUE, ...).
- `agg-apply-filter`: `APPLY "<expr>" AS name` computes a field. `FILTER "<expr>"` drops rows.
- `agg-sort-limit`: `SORTBY n @f ASC|DESC [MAX k]` and `LIMIT offset num`.
- `agg-load`: `LOAD n @f...` or `LOAD *` brings document fields into the pipeline.
- `agg-cursor`: `WITHCURSOR COUNT k`, then `FT.CURSOR READ idx <id>` until the cursor id is `0`, or `FT.CURSOR DEL`.

## How to get to it (user POV)

- `FT.AGGREGATE <index> "<query>" <steps...>` from any client.
- `FT.CURSOR READ|DEL <index> <cursor-id>` for paging.
- `FT.PROFILE <idx> AGGREGATE QUERY "<query>" <steps...>` for the result-processor chain.

## Driving it with rsv.sh

Preconditions:

- The same seeded standalone instance as [search.md](./search.md) (`idx` with `doc:1..3`).

- **Group and reduce.** Run `$R rec agg-groupby FT.AGGREGATE idx "*" GROUPBY 1 @tags REDUCE COUNT 0 AS n REDUCE AVG 1 @price AS avgp SORTBY 2 @n DESC`. You get three rows, one per distinct stored `tags` string: `sale,new` (avgp `30`), `new` (avgp `80`) and `sale` (avgp `15`), each with `n` `1`. Every `n` ties, so the row order is not guaranteed; assert the set of rows, not their order.
- **Apply and filter.** Run `$R rec agg-apply-filter FT.AGGREGATE idx "*" LOAD 1 @price APPLY "@price*2" AS dbl FILTER "@dbl>50"`. The rows are price `30` with dbl `60`, and price `80` with dbl `160`. Price `15` is filtered out.
- **Cursor.** Run `$R rec agg-cursor FT.AGGREGATE idx "*" LOAD 1 @title WITHCURSOR COUNT 2`. The reply holds 2 rows and a non-zero cursor id as its last element. Run `$R rec agg-cursor FT.CURSOR READ idx <id>` with that id. It returns the remaining row and cursor id `0`.

## Gotchas

- The leading integer in an `FT.AGGREGATE` reply is not a reliable row count. With `FILTER` it reported `1` while two rows came back. Count the rows.
- `GROUPBY @tags` groups by the stored field value. A hash value `sale,new` is one group, not split into tags. Users who expect per-tag groups need `APPLY split(...)`, which is a different feature.
- Fields not `SORTABLE` must be `LOAD`ed before `APPLY`/`FILTER` can see them. Otherwise expressions evaluate against a missing value.
- Cursors are per-index and expire after an idle timeout (`MAXIDLE`). Read them promptly in a recipe.
- Quote expressions: `"@price*2"` must reach the server as a single argument.
