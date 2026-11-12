# Add INDEXNULL

## Why

There is no way to query JSON documents whose field holds `null`
([#12307](https://github.com/RediSearch/RediSearch/issues/12307)):

- `ismissing(@f)` matches only documents where the field is absent; a `null`
  counts as present.
- `INDEXEMPTY` covers only the empty string.
- Composite workarounds such as `-@n:[-inf +inf]` or `-@t:{w'*'}` match a field
  that is `null` **or** absent, so they can't tell the two apart.

Applications that model "explicitly cleared" (`null`) separately from "never set"
(absent) — common with partial-update and patch semantics — have to mirror the
null-ness of each field into a separate indexed field of their own.

## What Changes

- A new field option, `INDEXNULL`, accepted on every field type (TEXT, TAG,
  NUMERIC, GEO, GEOSHAPE, VECTOR), in the same positions and combinations as
  `INDEXMISSING`. `FT.INFO` lists it on the fields that have it.
- A new query syntax, `isnull(@field)`, matching the documents in which the
  field holds a JSON `null`. It composes like any other node (`-isnull(@f)`,
  `isnull(@f) | ismissing(@f)`, …).
- A field holds `null` when its JSONPath yields a single `null` result. `null`s
  inside a multi-value result keep being skipped, as they are today.
- Following the `INDEXMISSING` and `INDEXEMPTY` precedents: `isnull` on a field
  without `INDEXNULL` is a query error; `INDEXNULL` with `NOINDEX` is rejected;
  disk-backed (Flex) indexes reject the option; there is no RDB encoding version
  bump.

### Non-goals

- Hashes: a hash field can't hold `null`, so `INDEXNULL` is accepted but never
  matches anything on a hash index.
- Treating `null`s inside multi-value results as making the field null.
- Disk-backed (Flex) support.
