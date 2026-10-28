# Remaining Geo Index port

## Scope and baseline

Port `src/geo_index.c` from master at
`58fba8cb56f5fcdeb91e1915db26974f6e469c07`. Geohash encoding, decoding,
distance calculation and range iteration are already Rust. This is an internal
implementation change, with no query syntax, persistence, or reply changes.

The characterization tests exercise the original production C before the port:
`GeoIndex.*`, `test_geo`, and `test_empty:testEmptyLegacyGeoFilters`.
They cover all four units, case folding, invalid units, embedded NUL and the
16-byte buffer limit, construction defaults, stable field indexes, validation
precedence and bounds, inclusive radius boundaries, distance output on a miss,
legacy cursor advancement, partial-parse cleanup, and dialect-dependent empty
arguments. Command parsing rejects NaN; comparison-only filter validation itself
accepts NaN and positive infinity. Do not turn that distinction into a new
validation policy or execute a NaN filter.

`geo_bencher` preserves an unchanged, symbol-renamed copy of the original C for
repeatable Criterion comparisons after production C is deleted. Compare unit
parsing, successful validation, and decoded-score radius checks on identical
inputs and release settings. The plugin freezes the C confidence intervals and
requires each Rust upper bound to be no greater than the C lower bound.

## Three independently buildable PRs

1. Add distance units and filter parameters to the existing pure `geo` crate,
   with public-API tests and property tests. Keep production C unchanged.
2. Add the C boundary under temporary Rust-prefixed symbols, including legacy
   parsing and ownership tests. Keep production callers on C. Preserve argument
   conversion through the existing `ArgsCursor` API: Redis strings and C strings
   intentionally have different numeric parsing rules.
3. Switch to the established C symbol names and delete `src/geo_index.c`.
   Retain the C-owned filter layouts in `geo_index.h`: numeric filters, query
   nodes, and disk interfaces embed/access them. Replacing these layouts is a
   separate migration, not required to remove the remaining implementation.

## Ownership and compatibility

Filter allocations stay with the Redis allocator, including legacy filters
allocated by the C request parser. Free a legacy allocation as the whole legacy
object, not as a Rust `Box<GeoFilter>`. Numeric-filter arrays are Rust allocations
and must continue through their existing Rust cleanup path. Hidden field names
are allocated only after successful parsing and freed through `HiddenString_Free`.

Preserve the first-error behavior and public/private error-message split of
`QueryError`. Keep the current default unit, unit numeric discriminants, field
index sentinel, and error ordering. No new finiteness policy, allocator change,
range algorithm, or schema-pointer retention is part of this port.

## Verification and review

The rust-port controller runs full builds, the focused C and flow suites, Rust
tests, Miri, lint/docs, and Criterion before publishing each stage. The final
stage also runs the Rust geo iterator and query-evaluation tests. On this macOS
runner the hash-only flow suite uses the existing virtualenv and `REJSON=0`;
RedisJSON is not a dependency of these tests. No checks are replaced by fabricated
summaries. Reviewers use `openai/gpt-6-astra`. Publication never merges the PRs.
