# Proposal: COLLECT Reducer

> **Status:** Proposal for review
> **Authors:** Itzik Vaknin
> **Reviewers:** Omer Shadmi, Adriano
> **Date:** 2026-03-23

---

## Background

After a discussion with @Omer Shadmi, we agreed (subject to Omer re-reviewing the PRD) that several aspects of the Enhanced TOLIST design point toward the wrong abstraction:

1. **No native way to get the Redis key.** `TOLIST *` returns the payload without `@__key`. RJ has an identifier in their payload, but other customers relying on the Redis key have no way to correlate grouped results back to source documents.

2. **Inconsistent response format.** `TOLIST @field` returns a flat array of scalars. `TOLIST *` returns an array of maps. Different shapes from the same reducer.

3. **No multi-field projections.** No middle ground between one field and everything.

These are symptoms of one root cause: **TOLIST offers no way to explicitly compose what goes into each entry.**

---

## Proposal

A new `COLLECT` reducer (name can be changed) that lets the user explicitly name the fields in each collected entry.

### Syntax

```
REDUCE COLLECT <narg> FIELDS <num_fields> <field_1> [<field_2> ...]
  [ALLOWDUPS]
  [SORTBY <inner_narg> (<@field> [ASC|DESC])+]
  [LIMIT <offset> <count>]
  AS <alias>
```

Each field can be: `@field_name`, `@__key`, `@__score`, `*` (full payload, nested), or any other supported option by RediSearch.

Each projected field becomes a **named pair** in the entry. Entry width is always `2 × num_fields`.

### Examples

**Key + score + payload, sorted and limited:**

```
FT.AGGREGATE idx:fruits *
  LOAD *
  GROUPBY 1 @color
    REDUCE COLLECT 12 FIELDS 3 @__key @__score * SORTBY 2 @sweetness DESC LIMIT 0 2 AS top_fruits
```

```
top_fruits =>
[
  ["__key",   "doc_10",
   "__score", "0.95",
   "*",       ["fruit", "apple", "color", "yellow", "sweetness", "6"]]
  ,
  ["__key",   "doc_1",
   "__score", "0.82",
   "*",       ["fruit", "banana", "color", "yellow", "sweetness", "5", "origin", "ecuador"]]
]
```

**Key + single field:**

```
REDUCE COLLECT 8 FIELDS 2 @__key @fruit SORTBY 2 @sweetness DESC LIMIT 0 3 AS top_fruits
```

```
→ [["__key", "doc_10", "fruit", "apple"],
   ["__key", "doc_1",  "fruit", "banana"],
   ["__key", "doc_7",  "fruit", "grape"]]
```

**Multi-field projection:**

```
REDUCE COLLECT 7 FIELDS 2 @title @rating SORTBY 2 @rating DESC LIMIT 0 3 AS top_movies
```

```
→ [["title", "Inception",  "rating", "8.8"],
   ["title", "The Matrix", "rating", "8.7"]]
```

---

## What This Solves

1. **Key availability** — `@__key` is just another field in the projection. No special flags.
2. **Consistent format** — every entry is always a flat array of named pairs. `*` is one named slot containing the nested payload.
3. **Multi-field projections** — pick exactly the fields you need.

---

## Decisions

| Question | Answer |
|----------|--------|
| ALLOWDUPS | Keep. Default = deep comparison of full projected entry. ALLOWDUPS skips it. |
| `@__score` | Supported — must be, since it's a valid SORTBY field. |
| Dedup scope | Full projected entry. Include `@__key` → every entry unique. Project only `@title` → dedup by title. |

---

WDYT?
