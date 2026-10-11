# Redis 8.10 JSONPath coverage

`tests/pytests/test_jsonpath_extensions.py` exercises OSS Search with RedisJSON
8.10 or newer. Older JSON modules are skipped by module version; failures on a
supported version are not skipped. Every test prints the Redis and module versions.

The predicate matrix checks TEXT, TAG and NUMERIC membership, JSON.GET versus
RETURN/LOAD values, writes before and after index creation, zero/one/multiple
matches, stale-value removal, and deletion. Additional tests cover sorting,
incompatible values and recovery, malformed paths and regexes,
missing/null/empty/mixed values, shared API classification, and RDB reloads.
This change adds coverage without changing Search's schema validation or evaluator.

## Usage and coverage

A JSONPath predicate selects stored nodes even when its condition computes a
value. For example, `$.rows[?(@.n + 1 == 3)].n` selects the stored `n`, so it
can be used for indexing, RETURN and LOAD.

A top-level computed projection such as `$.rows[0].n + 1` returns a calculated
value through JSON.GET. It cannot currently be evaluated through the RedisJSON
shared API used by Search. In RETURN/LOAD, the requested field is omitted while
the matching document or aggregate row remains. An empty field list is not a
request to return the full document.

Schema rejection tests use **aliased TEXT fields with offsets enabled**, where
Search already validates paths through the shared API. Other schema
configurations do not uniformly reject unsupported paths. This coverage does
not tighten their existing acceptance rules or claim that projections can be indexed.

In the table, **pass** means positive assertions; **reject** means an asserted
schema error in the configuration above; **omit** means asserted field omission
with the document/row retained. **N/A** denotes syntax used inside predicates,
rather than a standalone computed result.

| Expression family | JSONPath predicate: indexing | Same predicate path: RETURN / LOAD | Top-level computed result: aliased TEXT schema | Top-level computed result: RETURN / LOAD |
| --- | --- | --- | --- | --- |
| Addition, subtraction, multiplication, division, remainder; unary minus/plus | Pass | Pass | Reject | Omit |
| Array/object literal equality and inequality | Pass | Pass | N/A | N/A |
| Logical negation `!` | Pass | Pass | N/A | N/A |
| Membership `in` / `nin` | Pass | Pass | N/A | N/A |
| `size` / `sizeof` / `empty` | Pass | Pass | N/A | N/A |
| Infix `subsetof` / `anyof` / `noneof` | Pass | Pass | N/A | N/A |
| Function-form `subsetof()` / `anyof()` / `noneof()` | JSON.GET parity | JSON.GET parity | Reject | Omit |
| `length()` | Pass | Pass | Reject | Omit |
| `abs()` / `ceiling()` / `floor()` | Pass | Pass | Reject | Omit |
| `match()` / `search()` / `concat()` | Pass | Pass | Reject | Omit |
| `first()` / `last()` / `index()` / `append()` | Pass | Pass | Reject | Omit |
| `min()` / `max()` / `avg()` / `sum()` / `stddev()` | Pass | Pass | Reject | Omit |
| `keys()` / `~` | Pass | Pass | Reject | Omit |
| `count()` / `value()` | Pass | Pass | Reject | Omit |

Each positive predicate row checks TEXT, TAG and NUMERIC fields, initial and
subsequent writes, multiple matches, stale-value removal and deletion, and
compares RETURN/LOAD with JSON.GET.

The function-form relation tests accept either no matches or the correct match
from JSON.GET and require Search to agree. RedisJSON 8.10.0 produces no matches
for these function forms; the infix operators work. This parity check permits
future JSON implementations to supply the expected match. It does not establish
that silent nonmatching is intended behavior. The JSON team is tracking the discrepancy separately.

FT.AGGREGATE FILTER and APPLY use Search's own expression language. They do not
parse JSONPath. For example, loading `n` and using `APPLY '@n + 1' AS next` is a
separate way to compute a result. Tests of that language's existing operators,
functions and parser bugs are outside this JSONPath coverage.

## Shared API and follow-up work

The test-only `jsonpath_api.c` module calls the loaded RedisJSON V8 shared API.
`pathIsSingle` and `pathHasDefinedOrder` describe static path shape, not the
number of matches in a document. Tests classify static, wildcard, filter and
recursive paths, and verify rejection of computed projections and malformed paths.
The test compiles the module with `$CC` (default `cc`), loads it, and unloads it
in a `finally` block. The server must allow module loading and be able to read
the compiled module. No command or module is added to the Search product.

[MOD-19584](https://redislabs.atlassian.net/browse/MOD-19584) tracks computed
JSONPath projections through a versioned RedisJSON API and Search RETURN/LOAD
integration. Parsing permission alone is insufficient: computed results also
need evaluation support with defined ownership and lifetime. Indexing must
continue to select stored nodes. The projection tests here document current
omission; enabling projections will require updating those expectations.

This coverage adds no compiled-path cache or minimum shared API version.
[MOD-17022](https://redislabs.atlassian.net/browse/MOD-17022) and
[MOD-17229](https://redislabs.atlassian.net/browse/MOD-17229) cover query-time
and indexing-time caching. Those integrations should rerun these predicate,
projection, update and reload tests.

## Run against Redis 8.10

Build Search, then run with the Redis 8.10 server and its bundled RedisJSON
module. For an external container, mount the checkout and use a writable
`TMPDIR` visible to the server so it can load the test-only module.

```bash
REJSON=1 uv run --frozen --package rspytests python -m RLTest \
  --env existing-env --existing-env-addr localhost:18870 \
  --test tests/pytests/test_jsonpath_extensions.py \
  --test-timeout 20 --no-progress
```

The external server must load the built Search module and enable debug and
module commands. An isolated official `redis:8.10.0` image can also be used to
check parity with bundled Search; its entrypoint loads bundled modules automatically.
