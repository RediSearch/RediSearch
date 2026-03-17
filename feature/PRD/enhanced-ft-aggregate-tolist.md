# One Pager: Enhanced FT.AGGREGATE with Whole Document Fetching, In-Group Sorting, Limiting, and Deduplication Control (Raymond James)

> Source: [Confluence](https://redislabs.atlassian.net/wiki/spaces/DX/pages/5606178817)

## Problem Statement

Currently, `FT.AGGREGATE` supports grouping results but lacks the ability to:

1. Fetch the **complete document (all fields)** within each group.
2. **Sort** documents within a group based on a specific field.
3. **Limit** the number of documents returned per group.

Users who group data (for example, movies by genre) often need to retrieve the **full documents** belonging to each group, typically in **sorted order**, such as the _top N items by rating_.

Today, achieving this requires **multiple queries**: one to group and another to fetch full documents, increasing complexity and latency.

There is currently **no native way** to retrieve complete documents, apply SORTBY, and enforce LIMIT within a single aggregation query.

**Impacted Customer:** Raymond James

## Design Consideration

Currently, [TOLIST](https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/aggregations/#tolist) merges all _distinct_ values of a given property into a single array. It returns only unique entries, which might not always be the desired behavior.

The question then becomes: what if a user wants to retrieve all documents without de-duplication?

Removing the **distinct** behavior from the current TOLIST implementation would be a breaking change.

This raises another question, is there any real-world value in including identical documents within a merged list (i.e., without deduplication)?

In practice, almost never, though there are a few niche cases where it could be useful.

### 1. Event frequency or weighting

If identical docs represent _independent events_ (e.g., multiple clicks, identical orders, or identical sensor readings), keeping duplicates can encode **count or weight** information.

```json
[
  { "user_id": 123, "action": "view", "item": "A" },
  { "user_id": 123, "action": "view", "item": "A" }
]
```

These are identical structurally, but semantically _two distinct actions_. If we group and deduplicate, we lose the frequency.
→ Here, duplicates carry meaningful _cardinality_.

### 2. Audit or trace logs

In audit systems, the _fact that an identical record was logged multiple times_ may itself indicate redundancy, retries, or system issues.
→ Preserving identical docs helps diagnose replayed events or race conditions.

### 3. Statistical sampling or reproducibility

If your analysis is statistical (e.g., Monte Carlo simulations or model inputs), and identical entries occur by chance, removing them biases the sample.
→ Duplicates preserve the real input distribution.

## Proposed Feature

We propose extending the `TOLIST` reducer with additional capabilities to support fetching complete documents, as well as sorting and limiting items within each group.

This includes introducing a new property `*` to retrieve full documents, and an optional `ALLOWDUPS` flag to allow users to disable deduplication when desired.

### Purpose

The goal of this enhancement is to enable users to:

- Fetch **complete documents (all fields)** directly within an aggregation query.
- **Sort** and **limit** items within each group.
- Optionally control **deduplication behavior** by including or omitting the `ALLOWDUPS` flag.

By default, TOLIST will continue to return **distinct** values, ensuring backward compatibility.

### Syntax

```
REDUCE TOLIST narg <field|*> [ALLOWDUPS] 
  [SORTBY narg [<fields>] [ASC|DESC]] 
  [LIMIT <offset> <count>] 
  AS <alias>
```

### Behavior

- `*` returns all document fields (the complete document).
- `ALLOWDUPS` (optional) includes all documents, **even if identical**.
  - If omitted, TOLIST maintains current behavior and returns only **distinct** values.
- Can be used in combination with `LOAD` and `GROUPBY`.
- Supports sorting documents within each group by the specified field (e.g., `@rating DESC`).
- Supports limiting the number of documents per group using the `LIMIT` clause.
- Returns the resulting documents as a list under the specified alias.

### Example

```
FT.AGGREGATE idx:movies "*" 
LOAD * 
GROUPBY 1 @genre  
  REDUCE TOLIST 9 * ALLOWDUPS SORTBY 2 @rating DESC LIMIT 0 5 AS top_movies
SORTBY 1 @genre LIMIT 0 50
```

In this example:

- All documents are fetched (`*`).
- Duplicates are preserved because of the ALLOWDUPS flag.
- Each group (by `@genre`) is sorted by rating in descending order.
- Documents within each group are sorted by `@rating`.
- Only the top 5 documents per group are returned.
- Only top 50 groups are returned.

### RJ Use Case Example

```
FT.AGGREGATE idx:relationships "*" 
LOAD * 
GROUPBY 1 @relationshipName 
  REDUCE TOLIST 10 * SORTBY 4 @target DESC @bestByDate ASC LIMIT 0 3 AS all_distinct_docs
SORTBY 2 @relationshipId DESC
LIMIT 0 50
```

> A detailed example, along with the expected output format, is provided on [this page](https://redislabs.atlassian.net/wiki/spaces/DX/pages/5774410295).

## Requirements

1. Add support for `*` property in `TOLIST` to fetch complete documents.
2. Add optional `ALLOWDUPS` flag to control deduplication:
   - Default: distinct (deduplicated).
   - With `ALLOWDUPS`: non-deduplicated.
3. Extend TOLIST reducer to support:
   - `SORTBY <field> [ASC|DESC]`
   - `LIMIT <offset> <count>`
4. Ensure full backward compatibility with existing `TOLIST` behavior.
5. **Output Format:** Array of complete documents (JSON).

## Benefits

| **Category** | **Description** |
| --- | --- |
| **Completeness** | Enables returning full document contents directly in aggregation results. |
| **Flexibility** | Supports sorting and limiting within groups for top-N or recent-item use cases. |
| **Simplicity** | Removes the need for multiple queries or client-side joins to retrieve grouped documents. |
| **Backward Compatibility** | Retains current deduplicated behavior; `ALLOWDUPS` flag adds optional non-deduplicated mode. |
| **Parity with Elasticsearch** | Matches Elasticsearch's **top_hits** aggregation for sorting and limiting documents per group. |
| **Customer Value** | Simplifies reporting workflows for customers like Raymond James. |
