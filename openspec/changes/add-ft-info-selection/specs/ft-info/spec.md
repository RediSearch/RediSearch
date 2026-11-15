# FT.INFO selection

Syntax: `FT.INFO <index> [PREFIX <prefix> | FIELDS <field> [field ...]]`.

- No selector preserves current behavior.
- Selectors operate on top-level row names, using case-sensitive byte comparisons.
- Selected rows retain their full values, original order and RESP2 array / RESP3 map representation.
- FIELDS requires at least one name; unknown names and unavailable rows are errors. Duplicate names produce one row.
- PREFIX requires exactly one value; no match returns an empty collection and the empty prefix matches all rows.
- Selectors cannot be combined. Nested field names are not accepted as paths.
- The existing index lookup, ACL, alias and aggregation semantics apply. A requested field missing on any shard is an error, not a partial success. Existing shard errors remain errors.

Example: `FT.INFO idx FIELDS index_definition num_docs indexing percent_indexed hash_indexing_failures` includes the index's configured prefixes inside `index_definition`.
