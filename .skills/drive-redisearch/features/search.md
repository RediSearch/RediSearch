# Full-text and field search

`FT.SEARCH` takes a query in the RediSearch query language and returns matching document keys, optionally with fields. The query can combine full-text terms, tag sets, numeric ranges, prefixes and fuzzy terms. It supports sorting, paging, field projection and highlighting. An empty result and an invalid query are both well-formed replies.

## Sub-features

- `search-text`: term and field-scoped text (`hello`, `@title:shoes`), with stemming.
- `search-tag`: exact-match tags, `@tags:{sale}` and `@tags:{a|b}`.
- `search-numeric`: ranges `@price:[20 50]`, `(` for exclusive bounds, `-inf`/`+inf`.
- `search-prefix-fuzzy`: prefix `run*`, fuzzy `%shoez%` (Levenshtein distance 1 per `%` pair).
- `search-sort-page`: `SORTBY <field> ASC|DESC`, `LIMIT offset num`, `LIMIT 0 0` for a count only.
- `search-project`: `RETURN n f...`, `NOCONTENT`, `WITHSCORES`.
- `search-highlight`: `HIGHLIGHT FIELDS n f...` wraps matches in `<b>...</b>`.
- `search-empty`: no matches gives `(integer) 0` and no error.
- `search-syntax-error`: malformed query gives a `SEARCH_SYNTAX` error with an offset.
- `search-dialect`: `DIALECT 1` is the default (`DEFAULT_DIALECT_VERSION` in `src/config.h`, overridable with the `DEFAULT_DIALECT` module arg). `DIALECT 2` is needed for `$param` references and changes how several operators parse.

## How to get to it (user POV)

- `FT.SEARCH <index|alias> "<query>" [options]` from any client.
- `FT.EXPLAIN` / `FT.EXPLAINCLI` show the parsed query tree, which helps when a query matches unexpectedly.
- `FT.PROFILE <idx> SEARCH QUERY "<query>"` returns the results plus the iterator tree and timings.

## Driving it with rsv.sh

Preconditions:

- Standalone instance, `FLUSHALL`ed, then seeded with:
  `$R rec search-seed FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT price NUMERIC SORTABLE tags TAG`,
  `$R rec search-seed HSET doc:1 title "red running shoes" price 30 tags sale,new`,
  `$R rec search-seed HSET doc:2 title "blue shoes" price 80 tags new`,
  `$R rec search-seed HSET doc:3 title "red hat" price 15 tags sale`.

- **Text + sort + project.** Run `$R rec search-text FT.SEARCH idx "@title:shoes" SORTBY price ASC RETURN 2 title price`. The reply is `(integer) 2`, then `doc:1` (price `30`) before `doc:2` (price `80`).
- **Tag.** Run `$R rec search-tag FT.SEARCH idx "@tags:{sale}" NOCONTENT`. The reply is `(integer) 2` with `doc:1` and `doc:3`.
- **Numeric range.** Run `$R rec search-numeric FT.SEARCH idx "@price:[20 50]" NOCONTENT`. The reply is `(integer) 1`, `doc:1`.
- **Prefix and fuzzy.** Run `$R rec search-prefix-fuzzy FT.SEARCH idx "run*" NOCONTENT`, which returns `doc:1`. Then run `$R rec search-prefix-fuzzy FT.SEARCH idx "%shoez%" NOCONTENT`, which returns `(integer) 2`.
- **Highlight.** Run `$R rec search-highlight FT.SEARCH idx red HIGHLIGHT FIELDS 1 title RETURN 1 title`. Titles come back as `"<b>red</b> hat"` and `"<b>red</b> running shoes"`.
- **Empty.** Run `$R rec search-empty FT.SEARCH idx volcano`. The reply is `(integer) 0` and contains no `(error)`.
- **Syntax error.** Run `$R rec search-syntax-error FT.SEARCH idx "@title:("`. The reply is `(error) SEARCH_SYNTAX Syntax error at offset 7 near title`, and the artifact is tagged `ERROR REPLY`.
- **Count only.** Run `$R cli FT.SEARCH idx shoes LIMIT 0 0`. The reply is `1) (integer) 2` with no keys.
- **Profile as observation.** Run `$R cli FT.PROFILE idx SEARCH QUERY "red shoes" NOCONTENT`. The first element is the normal result (`doc:1`). Under `Iterators profile`, the top iterator is `INTERSECT` with `TEXT` term `red` and a `UNION` for the stemmed `shoes` expansions.

## Gotchas

- A missing `RETURN`/`NOCONTENT` returns every field. That works, but the replies are noisy and hide the assertion.
- Stemming means `shoes` also matches `shoe`. Use `VERBATIM` when the assertion is about exact terms.
- Default dialect matters. `$param` references and some operators parse differently under `DIALECT 1`, so state the dialect in the command whenever the change involves the parser.
- `SORTBY` on a field that is not declared `SORTABLE` still works, but takes a different (loading) code path. If the change touches sorting, test both.
- Tag separators default to `,`. A hash value `sale,new` gives two tags.
- `redis-cli` exits 0 for `(error)` replies. Check the reply text or the `rec` `ERROR REPLY` tag.
