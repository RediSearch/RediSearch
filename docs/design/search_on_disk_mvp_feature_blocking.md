# Search on Disk MVP: Feature Blocking Status

This document maps the features that should be blocked/disallowed for Disk mode according to the
[Search on Disk MVP Feature Map](https://redislabs.atlassian.net/wiki/spaces/DX/pages/5143920764/Search+on+Disk+-+MVP+Feature+Map)
and compares them against the actual implementation status in the codebase.

---

## Quick Reference Summary

### Commands Allowed in SearchDisk (Flex) Mode - MVP

| Command | Status | Notes |
|---------|--------|-------|
| `FT.CREATE` | ✅ Allowed | Requires `SKIPINITIALSCAN`; only HASH type; only TEXT/TAG/VECTOR fields |
| `FT.DROPINDEX` | ✅ Allowed | `DD` option blocked |
| `FT.SEARCH` | ✅ Allowed | Requires `NOCONTENT` or `RETURN 0`; no SLOP/INORDER/HIGHLIGHT/SUMMARIZE/SORTBY/LOAD |
| `FT.PROFILE SEARCH` | ✅ Allowed | Only with `SEARCH` subcommand |
| `FT.INFO` | ✅ Allowed | — |
| `FT._LIST` | ✅ Allowed | — |
| `FT.EXPLAIN` | ✅ Allowed | — |
| `FT.EXPLAINCLI` | ✅ Allowed | — |
| `FT.ALIASADD` | ✅ Allowed | — |
| `FT.ALIASUPDATE` | ✅ Allowed | — |
| `FT.ALIASDEL` | ✅ Allowed | — |
| `FT.CONFIG` | ✅ Allowed | — |
| `FT.DEBUG` | ✅ Allowed | — |

### Commands Blocked in SearchDisk (Flex) Mode - MVP

| Command | Status | Reason |
|---------|--------|--------|
| `FT.AGGREGATE` | ❌ Blocked | Not supported |
| `FT.HYBRID` | ❌ Blocked | Not supported |
| `FT.CURSOR READ/DEL/PROFILE/GC` | ❌ Blocked | No cursor support |
| `FT.ALTER` | ❌ Blocked | Schema modification not supported |
| `FT.DROP` | ❌ Blocked | Use `FT.DROPINDEX` instead |
| `FT.DROPINDEX DD` | ❌ Blocked | Document deletion not supported |
| `FT.DICTADD` | ❌ Blocked | Dictionary not supported |
| `FT.DICTDEL` | ❌ Blocked | Dictionary not supported |
| `FT.DICTDUMP` | ❌ Blocked | Dictionary not supported |
| `FT.SUGADD` | ❌ Blocked | Suggestions not supported |
| `FT.SUGGET` | ❌ Blocked | Suggestions not supported |
| `FT.SUGDEL` | ❌ Blocked | Suggestions not supported |
| `FT.SUGLEN` | ❌ Blocked | Suggestions not supported |
| `FT.SYNUPDATE` | ❌ Blocked | Synonyms not supported |
| `FT.SYNDUMP` | ❌ Blocked | Synonyms not supported |
| `FT.SYNADD` | ❌ Blocked | Synonyms not supported (deprecated) |
| `FT.SPELLCHECK` | ❌ Blocked | Not supported |

### Deprecated Commands (Blocked)

| Command | Status | Notes |
|---------|--------|-------|
| `FT.DROP` | ❌ Blocked | Use `FT.DROPINDEX` instead |
| `FT.MGET` | ❌ Blocked | Deprecated, not supported |
| `FT.ADD` | ❌ Blocked | Deprecated, use HSET instead |
| `FT.SAFEADD` | ❌ Blocked | Deprecated, use HSET instead |
| `FT.DEL` | ❌ Blocked | Deprecated, use DEL instead |
| `FT.GET` | ❌ Blocked | Deprecated, use HGETALL instead |
| `FT.TAGVALS` | ❌ Blocked | Deprecated, not supported |
| `FT.SYNADD` | ❌ Always Error | Deprecated, returns error regardless |

### Future Enhancements (Post-MVP)

Once the MVP is complete, the following features should be unblocked:

| Feature | Current Status | Target Status |
|---------|----------------|---------------|
| `NUMERIC` field type | ❌ Blocked | ✅ Allow |
| `GEO` field type | ❌ Blocked | ✅ Allow |
| `GEOSHAPE` field type | ❌ Blocked | ✅ Allow |
| `FT.AGGREGATE` | ❌ Blocked | ✅ Allow |
| `FT.HYBRID` | ❌ Blocked | ✅ Allow |
| `FT.CURSOR` commands | ❌ Blocked | ✅ Allow |
| `FT.ALTER` | ❌ Blocked | ✅ Allow |
| `SORTBY` argument | ❌ Blocked | ✅ Allow |
| `ON JSON` | ❌ Blocked | ✅ Allow |
| Vector Range queries | ⚠️ Allowed | ✅ Keep allowed |
| `FLAT` vector algorithm | ❌ Blocked | ⚠️ TBD |
| `SVS` vector algorithm | ❌ Blocked | ⚠️ TBD |
| Infix/Suffix/Wildcard/Fuzzy | ⚠️ Returns empty | ⚠️ TBD |

---

## Legend

- ✅ **BLOCKED** - Feature is properly blocked in code
- ❌ **NOT BLOCKED** - Feature should be blocked but isn't
- ⚠️ **PARTIAL** - Partially blocked or unclear
- ➖ **N/A** - Not applicable or implicitly blocked

---

## 1. Index Field Types

| Feature | Should Block? | Actually Blocked? | Code Location |
|---------|---------------|-------------------|---------------|
| NUMERIC field | ✅ Yes | ✅ BLOCKED | `spec.c:1367` - `SearchDisk_MarkUnsupportedFieldIfDiskEnabled(SPEC_NUMERIC_STR, ...)` |
| GEO field | ✅ Yes | ✅ BLOCKED | `spec.c:1373` - `SearchDisk_MarkUnsupportedFieldIfDiskEnabled(SPEC_GEO_STR, ...)` |
| GEOSHAPE field | ✅ Yes | ✅ BLOCKED | `spec.c:1360` - `SearchDisk_MarkUnsupportedFieldIfDiskEnabled(SPEC_GEOMETRY_STR, ...)` |
| TEXT field | No | ➖ Allowed | — |
| TAG field | No | ➖ Allowed | — |
| VECTOR field | No | ➖ Allowed | — |

---

## 2. FT.CREATE Arguments

| Argument | Should Block? | Actually Blocked? | Code Location |
|----------|---------------|-------------------|---------------|
| `ON JSON` | ✅ Yes | ✅ BLOCKED | `spec.c:1761-1770` - `invalid_flex_on_type` check |
| `NOOFFSETS` | ✅ Yes | ✅ BLOCKED | `spec.c:1741-1759` - Not in `flex_argopts`, triggers error |
| `NOHL` | ✅ Yes | ✅ BLOCKED | `spec.c:1741-1759` - Not in `flex_argopts` |
| `NOFIELDS` | ✅ Yes | ✅ BLOCKED | `spec.c:1741-1759` - Not in `flex_argopts` |
| `NOFREQS` | ✅ Yes | ✅ BLOCKED | `spec.c:1741-1759` - Not in `flex_argopts` |
| `MAXTEXTFIELDS` | ✅ Yes | ✅ BLOCKED | `spec.c:1741-1759` - Not in `flex_argopts` |
| `ASYNC` | ✅ Yes | ✅ BLOCKED | `spec.c:1741-1759` - Not in `flex_argopts` |
| Missing `SKIPINITIALSCAN` | ✅ Yes | ✅ BLOCKED | `spec.c` - Requires SKIPINITIALSCAN for Flex |
| `WITHSUFFIXTRIE` | ✅ Yes | ✅ BLOCKED | `spec.c:1141-1148,1181-1188` - Blocked in `parseTextField`/`parseTagField` |

---

## 3. FT.SEARCH Arguments

| Argument | Should Block? | Actually Blocked? | Code Location |
|----------|---------------|-------------------|---------------|
| Without `NOCONTENT` or `RETURN 0` | ✅ Yes | ✅ BLOCKED | `aggregate_request.c:764-767` |
| `LOAD` | ✅ Yes | ✅ BLOCKED | `aggregate_request.c:1004-1008` |
| `SLOP` | ✅ Yes | ✅ BLOCKED | `aggregate_request.c:698-704` |
| `INORDER` | ✅ Yes | ✅ BLOCKED | `aggregate_request.c:705-708` |
| `HIGHLIGHT` | ✅ Yes | ✅ BLOCKED | `aggregate_request.c:639-642` |
| `SUMMARIZE` | ✅ Yes | ✅ BLOCKED | `aggregate_request.c:627-630` |
| `GEOFILTER` | ✅ Implicit | ✅ BLOCKED (implicit) | GEO field type is blocked, so no GEO index |
| `FILTER` (numeric) | ✅ Implicit | ✅ BLOCKED (implicit) | NUMERIC field type is blocked |
| `SORTBY` | ✅ Yes | ✅ BLOCKED | `aggregate_request.c:310-312` |

### Scorer Reference

The following table lists all available scorers and their Disk mode compatibility:

| Scorer | Should Block? | Actually Blocked? | Reason | Code Location |
|--------|---------------|-------------------|--------|---------------|
| `TFIDF` | ✅ Yes | ✅ BLOCKED | Uses SLOP in calculation (`tfidf /= slop`) | `aggregate_request.c:1433-1437`, `default.c:116-117` |
| `TFIDF.DOCNORM` | ✅ Yes | ✅ BLOCKED | Uses SLOP (same as TFIDF, different normalization) | `aggregate_request.c:1438-1442`, `default.c:116-117` |
| `BM25` | ✅ Yes | ✅ BLOCKED | Uses SLOP in calculation (`score /= slop`) - **deprecated** | `aggregate_request.c:1443-1447`, `default.c:211-212` |
| `BM25STD` | No | ➖ Allowed | **Default scorer** - does not use SLOP | — |
| `BM25STD.TANH` | No | ➖ Allowed | Normalized BM25STD with tanh - does not use SLOP | — |
| `BM25STD.NORM` | No | ➖ Allowed | Normalized BM25STD - does not use SLOP | — |
| `DISMAX` | No | ➖ Allowed | Sum of term frequencies - does not use SLOP | — |
| `DOCSCORE` | No | ➖ Allowed | Returns raw document score only - does not use SLOP | — |
| `HAMMING` | No | ➖ Allowed | Hamming distance scorer - does not use SLOP | — |

**Note:** Scorers that use SLOP require term offset information to calculate proximity between matched terms.
Since SLOP is blocked for Disk mode (see above), any scorer that relies on SLOP must also be blocked.

---

## 4. Commands - Completely Blocked

| Command | Should Block? | Actually Blocked? | Code Location |
|---------|---------------|-------------------|---------------|
| `FT.AGGREGATE` | ✅ Yes | ✅ BLOCKED | `module.c:1780,4662` - `DiskDisabledCmd(RSAggregateCommand)` |
| `FT.HYBRID` | ✅ Yes | ✅ BLOCKED | `module.c:1779,4665` - `DiskDisabledCmd(RSShardedHybridCommand)` |
| `FT.CURSOR` (all) | ✅ Yes | ✅ BLOCKED | `module.c:3804-3832` - All cursor commands |
| `FT.ALTER` | ✅ Yes | ✅ BLOCKED | `module.c:1753,4677` - `DiskDisabledCmd(AlterIndexCommand)` |
| `FT.DICTADD` | ✅ Yes | ✅ BLOCKED | `module.c:1755,4682` - `DiskDisabledCmd(DictAddCommand)` |
| `FT.DICTDEL` | ✅ Yes | ✅ BLOCKED | `module.c:1756,4683` - `DiskDisabledCmd(DictDelCommand)` |
| `FT.DICTDUMP` | ✅ Yes | ✅ BLOCKED | `module.c:1771` - `DiskDisabledCmd(DictDumpCommand)` |
| `FT.SUGADD` | ✅ Yes | ✅ BLOCKED | `module.c:1764` - `DiskDisabledCmd(RSSuggestAddCommand)` |
| `FT.SUGGET` | ✅ Yes | ✅ BLOCKED | `module.c:1765` - `DiskDisabledCmd(RSSuggestGetCommand)` |
| `FT.SUGDEL` | ✅ Yes | ✅ BLOCKED | `module.c:1766` - `DiskDisabledCmd(RSSuggestDelCommand)` |
| `FT.SUGLEN` | ✅ Yes | ✅ BLOCKED | `module.c:1767` - `DiskDisabledCmd(RSSuggestLenCommand)` |

---

## 5. Vector Index Features

| Feature | Should Block? | Actually Blocked? | Code Location |
|---------|---------------|-------------------|---------------|
| FLAT algorithm | ✅ Yes | ✅ BLOCKED | `spec.c:1228-1236` - Error for FLAT on disk |
| SVS algorithm | ✅ Yes | ✅ BLOCKED | `spec.c:1271-1278` - Error for SVS on disk |
| Range query | ⚠️ Per doc | ❌ NOT BLOCKED | `vector_index.c:155-173` - Range query still allowed |
| Multi-value vectors | ✅ Implicit | ✅ BLOCKED (implicit) | JSON is blocked |

---

## 6. Query Syntax Features

| Feature | Should Block? | Actually Blocked? | Code Location |
|---------|---------------|-------------------|---------------|
| Infix search (`*foo*`) | ⚠️ Per doc | ❌ NOT BLOCKED | Will return empty results (no suffix trie) |
| Suffix search (`*foo`) | ⚠️ Per doc | ❌ NOT BLOCKED | Will return empty results (no suffix trie) |
| Wildcard (`w'pattern'`) | ⚠️ Per doc | ❌ NOT BLOCKED | Will return empty results |
| Fuzzy search (`%term%`) | ⚠️ Per doc | ❌ NOT BLOCKED | Will return empty results |
| Numeric range queries | ✅ Implicit | ✅ BLOCKED (implicit) | NUMERIC field blocked |
| Geo queries | ✅ Implicit | ✅ BLOCKED (implicit) | GEO field blocked |

---

## 7. Configuration / Limits

| Feature | Should Block? | Actually Blocked? | Code Location |
|---------|---------------|-------------------|---------------|
| Max 10 indexes | ✅ Yes | ✅ BLOCKED | `search_disk_utils.c:13-18` - `FLEX_MAX_INDEX_COUNT` check |
| WORKERS = 0 | ✅ Yes | ✅ BLOCKED | Corrected to 1 automatically |

---
