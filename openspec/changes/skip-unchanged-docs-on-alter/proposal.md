# Skip Documents Without Newly Added Fields on FT.ALTER

Jira: MOD-18171
Design doc: https://redislabs.atlassian.net/wiki/spaces/DX/pages/6688243734/FT.ALTER+Selective+Reindexing+and+Vector+Relabeling

## Why

`FT.ALTER ... SCHEMA ADD` scans every document matching the index and fully reindexes it, even
when the document contains none of the fields added by the command. Loading every schema field
and replacing every matching document makes the backfill unnecessarily expensive when the new
fields are sparse.

## What Changes

Optimize only the backfill started by `FT.ALTER ... SCHEMA ADD`:

- For each matching Hash or JSON document, first check only the fields added by that command.
- If none of those fields are present, leave the document and its existing index entries untouched.
- If any added field is present, use the existing full-reindex path without changing it.
- Use the existing full scan for the entire backfill when an added field uses `INDEXMISSING` or
  `SORTABLE`, when another per-index scan is active, when an earlier background scan failed, or when
  the index has `SKIPINITIALSCAN` history. Disk-backed specs are excluded defensively; `FT.ALTER` is
  already rejected under Flex.

This is a conservative shortcut around the existing reindex path. It does not change
`FT.ALTER` syntax, `SKIPINITIALSCAN`, the persistence format, initial index scans, or other callers
of the background scanner. The indexed field values are unchanged.

Partial field-level reindexing, vector relabeling, and changes to cancellation or OOM recovery are
out of scope.
