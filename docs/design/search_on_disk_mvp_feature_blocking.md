# Search on Disk MVP: Feature Blocking Status

This document maps the features that should be blocked/disallowed for Disk mode according to the
[Search on Disk MVP Feature Map](https://redislabs.atlassian.net/wiki/spaces/DX/pages/5143920764/Search+on+Disk+-+MVP+Feature+Map)
and compares them against the actual implementation status in the codebase.

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
| `WITHSUFFIXTRIE` | ⚠️ Per doc | ❌ NOT BLOCKED | Still accepted in `parseTextField`/`parseTagField` |

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
| `SCORER TFIDF` | No | ➖ Allowed | TFIDF scorer is supported |

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
