# alter-selective-backfill (delta)

On merge, the requirements below are folded into `openspec/specs/alter-selective-backfill/spec.md`.

## ADDED Requirements

### Requirement: Skip full reindex for documents missing every field added by FT.ALTER
When `FT.ALTER <index> SCHEMA ADD` adds one or more fields and schedules its background backfill
scan, the scan SHALL skip full reindexing of a matching document when none of the fields added by
that command are present on the document, provided the shortcut is eligible for this scan (see
below). A skipped document's internal ID and existing index entries SHALL remain unchanged.

#### Scenario: Sparse added field, Hash document
- **GIVEN** an index over Hash documents with an existing field `title`
- **AND** `FT.ALTER idx SCHEMA ADD tags TAG` is executed
- **AND** a document `doc:1` has `title` but no `tags`
- **WHEN** the backfill scan visits `doc:1`
- **THEN** `doc:1` SHALL NOT be reindexed
- **AND** `doc:1`'s internal document ID SHALL be unchanged after the scan completes

#### Scenario: Added field present, Hash document
- **GIVEN** the same index and ALTER as above
- **AND** a document `doc:2` has both `title` and `tags`
- **WHEN** the backfill scan visits `doc:2`
- **THEN** `doc:2` SHALL be fully reindexed via the existing replace path
- **AND** the resulting index entries for `doc:2` SHALL be identical to what today's unconditional
  full-reindex backfill would have produced

#### Scenario: Added field present via Hash alias
- **GIVEN** an index with a field added as `FT.ALTER idx SCHEMA ADD category AS cat TAG`
- **AND** a Hash document has a `category` key but no `cat`-named key
- **WHEN** the backfill scan visits the document
- **THEN** presence SHALL be resolved through the stored field path (`category`), not the alias
  (`cat`), matching the full-reindex path

#### Scenario: Added field present via nested or multi-value JSONPath
- **GIVEN** an index over JSON documents with a field added at a nested or multi-value JSONPath
- **AND** a document's JSON value has at least one match for that path
- **WHEN** the backfill scan visits the document
- **THEN** the document SHALL be treated as having the added field present, and SHALL be fully
  reindexed via the existing replace path

### Requirement: Conservative fallback to full reindexing
The selective skip SHALL NOT apply — the scan SHALL fall back to reindexing every matching
document, as it does today — for any `FT.ALTER ... SCHEMA ADD` backfill where any of the following
hold:

- Any field added by the command has `INDEXMISSING` set.
- Any field added by the command has `SORTABLE` set.
- The index is disk-backed (unreachable today, since `FT.ALTER` is rejected under Flex; listed so
  the gate stays explicit if that changes).
- The index has another background scan already active or pending when this `ALTER` is scheduled.
- The index's last background scan ended in an unresolved out-of-memory failure.
- The index was created with `SKIPINITIALSCAN`, or an earlier `FT.ALTER ... SKIPINITIALSCAN` on
  this index added fields without ever running a backfill for them.

If an individual document's field-presence probe fails, that document SHALL use the full-reindex
path rather than being treated as missing all added fields. Other documents in the same eligible
scan MAY still use the selective skip.

#### Scenario: INDEXMISSING disables the shortcut
- **GIVEN** `FT.ALTER idx SCHEMA ADD status TAG INDEXMISSING`
- **WHEN** the backfill scan runs
- **THEN** every matching document SHALL be fully reindexed via the existing replace path,
  regardless of whether `status` is present

#### Scenario: SORTABLE disables the shortcut
- **GIVEN** `FT.ALTER idx SCHEMA ADD rank NUMERIC SORTABLE`
- **WHEN** the backfill scan runs
- **THEN** every matching document SHALL be fully reindexed via the existing replace path, so that
  every document's sorting vector is rebuilt for the widened schema

#### Scenario: Prior SKIPINITIALSCAN forces a full scan on a later ALTER
- **GIVEN** `FT.ALTER idx SKIPINITIALSCAN SCHEMA ADD a TAG` completed without running a backfill
- **AND** the server was restarted, reloading the index from RDB
- **WHEN** a later `FT.ALTER idx SCHEMA ADD b TAG` is executed (without `SKIPINITIALSCAN`)
- **THEN** its backfill scan SHALL fully reindex every matching document, including documents that
  have `a` but not `b`

#### Scenario: Probe failure falls through to full reindex
- **GIVEN** `FT.ALTER idx SCHEMA ADD tags TAG`
- **AND** the presence probe for a specific document cannot determine whether `tags` is present
- **WHEN** the backfill scan visits that document
- **THEN** the document SHALL be fully reindexed via the existing replace path, not skipped

### Requirement: No change to FT.ALTER's observable command surface
`FT.ALTER`'s syntax, arguments, replies, and error messages SHALL be unchanged by this backfill
optimization. `SKIPINITIALSCAN` SHALL continue to mean "do not run a backfill scan for this ALTER."
The final set of documents indexed, and the searchable content of any document that is reindexed,
SHALL be identical to what the unconditional full-reindex backfill produces today.

#### Scenario: Coordinator and debug scan controls unaffected
- **GIVEN** a coordinator-fronted deployment or a test using `FT.DEBUG` scan pause/OOM controls
- **WHEN** `FT.ALTER ... SCHEMA ADD` is executed
- **THEN** coordinator fan-out behavior and existing debug scan controls SHALL behave exactly as
  they did before this change
