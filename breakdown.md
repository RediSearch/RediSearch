# COLLECT Reducer - Task Breakdown

## Scope

Design doc §1–6, 9, 10. **Excluded:** §7, §8 (deferred optimizations).

## Key Design Decision

JSON dedup uses string comparison (§9.2 decision). HASH dedup needs positional equality — isolated as the final deliverable.

## Dependencies

```
                 ┌──► 3. * support ──────────────────────┐
1. Standalone ──►├──► 2. Cluster                          ├──► 6. Finalization
                 └──► 4. JSON dedup ──► 5. HASH dedup ──┘
```

Steps 2, 3, 4 can start after 1 (parallel). 3 needs 2 for cluster `*`. Step 5 depends on 4. Step 6 after all deliverables.

## Tasks

### Deliverable 1: Standalone JSON, no dedup

| Task | Description | DOD |
|---|---|---|
| 1.1 | **COLLECT parser** — Parse COLLECT clause (FIELDS, SORTBY, LIMIT, AS, DEDUP token, narg counting). Validate constraints. Register COLLECT as a reducer type. | Valid clauses accepted, invalid rejected with clear error. Reducer instantiated from pipeline. |
| 1.2 | **Reducer — projection only** — Add(): capture projected field values from source RLookupRow, IncrRef all RSValue*. Finalize(): build nested KV map output (2 × num_fields wide). No sorting, no limiting. | Standalone JSON COLLECT returns correct projected fields. E2E tests pass. |
| 1.3 | **SORTBY + LIMIT** — Heap path (SORTBY present): bounded min-max heap insert with comparison, pop sorted via mmh_pop_max(). Array path (SORTBY absent): growing array, insertion order. LIMIT offset/count applied in Finalize(). | All SORTBY/LIMIT combos correct for standalone JSON. E2E tests pass. |

### Deliverable 2: Cluster JSON, no dedup

| Task | Description | DOD |
|---|---|---|
| 2.1 | **Coordinator entry decomposition** — Shard Finalize() serializes projected fields + auxiliary sort keys on wire. Distribution passes COLLECT clause through unmodified. Coordinator: build inner RLookup at creation (keys for FIELDS + SORTBY), unpack shard entry maps into temporary RLookupRow, call shared heap-insertion, strip aux sort keys in Finalize(). | Multi-shard queries return correct merged/sorted/limited results. E2E cluster tests pass. |
| | *Subtask 2.1a:* Inner RLookup + map-to-RLookupRow unpacking function (impl) | |
| | *Subtask 2.1b:* Coordinator Add()/Finalize() using entry decomposition (integrate) | |

### Deliverable 3: `*` field support, no dedup

| Task | Description | DOD |
|---|---|---|
| 3.1 | **`*` field support** — Recognize `*` in FIELDS list. HASH: load all fields as flat map. JSON: load root as nested RSValue_Map (Trio). Output as nested map. Cluster: serialize nested map on wire, coordinator unpacks through entry decomposition. | `*` returns full doc alongside named fields, standalone + cluster, HASH + JSON. E2E tests pass. |

### Deliverable 4: JSON dedup

| Task | Description | DOD |
|---|---|---|
| 4.1 | **JSON dedup** — Implement dedup_and_insert() (§9.1): early-exit if can't survive heap, scan for duplicate projected fields via string comparison, keep best sort-key representative. `@__key` skip optimization. Integrate into Add() when DEDUP flag set. Standalone + cluster. | JSON DEDUP removes string-equal duplicates. E2E tests pass standalone + cluster. |
| | *Subtask 4.1a:* dedup_and_insert() algorithm (impl). @__key optimization is optional. | |
| | *Subtask 4.1b:* Integrate into Add() (integrate), standalone + cluster | |

### Deliverable 5: HASH dedup

| Task | Description | DOD |
|---|---|---|
| 5.1 | **HASH dedup** — Positional equality: iterate projected fields, delegate to polymorphic RSValue_Cmp per value. Plug into dedup path for HASH fields. Standalone + cluster. | HASH DEDUP removes equal duplicates. E2E tests pass standalone + cluster. |
| | *Subtask 5.1a:* Positional equality comparison function (impl) | |
| | *Subtask 5.1b:* Integrate into dedup path (integrate) | |

### 6. Finalization

| Task | Description | DOD |
|---|---|---|
| 6.1 | **Edge case coverage** — Empty groups, NULL/missing fields, single-element groups, COLLECT alongside other reducers (COUNT, SUM, TOLIST), multiple COLLECT reducers in same GROUPBY. | Edge case tests pass standalone + cluster. |
| 6.2 | **Stress tests** — Large number of groups, large group sizes, high LIMIT values, many projected fields. | No crashes, memory leaks, or correctness issues under load. |
| 6.3 | **Benchmarks + profiling** — Run benchmarks and profiling across representative COLLECT workloads. Map costly operations. Identify easy-to-optimize points. | Results documented with identified hotspots and optimization candidates. |
| 6.4 | **Docs + examples** — Document COLLECT syntax, options, response format. Include examples for common use cases (top-K per group, multi-field projection, dedup). | Documentation published. |
| 6.5 | **Integration with FT.HYBRID, FT.PROFILE, FT.EXPLAIN** — Verify COLLECT works correctly with hybrid queries, profile output, and explain output. | Test coverage for each integration point. |
| 6.6 | **Real-world scenario tests** — Real-world use case tests (e.g. Raymond James pattern). Multiple COLLECT reducers in same GROUPBY. Multiple GROUPBY stages. COLLECT alongside other reducers. | E2E tests pass for all scenarios. |
| 6.7 | **Client libraries** — Notify clients team to add COLLECT support to client SDKs. | Clients team notified with syntax spec and examples. |

## Open Items (resolved as we go)

| Item | Relevant Task |
|---|---|
| Null/missing field handling | 1.2 |
