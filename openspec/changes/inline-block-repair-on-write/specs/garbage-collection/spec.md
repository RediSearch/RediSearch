# garbage-collection (delta)

> On merge, the requirements below would be folded into
> `openspec/specs/garbage-collection/spec.md`.

## ADDED Requirements

### Requirement: Inline repair of the tail block on write

When a writer appends a posting to an inverted index, RediSearch SHALL be able to reclaim
entries in that index's last block belonging to documents that no longer exist, in the same
operation as the write, without forking a child process.

Inline repair SHALL be confined to the last block of the index being written. It SHALL NOT
visit or modify any other block.

#### Scenario: Tail block has no dead entries
- **WHEN** a document is indexed into a term whose last block contains no entries for deleted documents
- **THEN** the index SHALL be unchanged apart from the appended posting
- **AND** the index's GC marker SHALL NOT be incremented

#### Scenario: Tail block is partly dead
- **WHEN** a term's last block contains entries for deleted documents at or above the configured threshold
- **AND** a document is indexed into that term
- **THEN** the entries for deleted documents SHALL be removed from that block
- **AND** the surviving entries SHALL decode to the same records, in the same order, as before the repair
- **AND** the index's GC marker SHALL be incremented

#### Scenario: Tail block is entirely dead
- **WHEN** every entry in a term's last block belongs to a deleted document
- **AND** a document is indexed into that term
- **THEN** that block SHALL be removed from the index
- **AND** the index's block count SHALL be reduced accordingly

#### Scenario: Accounting agrees with the fork GC
- **WHEN** an index is repaired inline
- **AND** an identical index is repaired by a fork-GC scan and apply
- **THEN** both indexes SHALL report the same unique document count, the same entry count, and the same decoded contents

#### Scenario: Queries are unaffected by a concurrent repair
- **WHEN** a query is executing against an index whose tail block is repaired inline during that execution
- **THEN** the query SHALL return the same result set it would return with no concurrent write
- **AND** no result SHALL be returned twice

#### Scenario: Persistence round-trip after inline repair
- **WHEN** an index has been repaired inline
- **AND** the index is saved to RDB and reloaded
- **THEN** the reloaded index SHALL contain the same documents and return the same query results

### Requirement: Inline repair is configurable and off by default

RediSearch SHALL expose a runtime configuration controlling the dead-entry proportion at which
inline repair triggers. The value `0` SHALL disable inline repair entirely. The default SHALL
be `0`.

#### Scenario: Disabled by default
- **WHEN** a server is started with no explicit inline-repair configuration
- **AND** documents are indexed and deleted
- **THEN** no inline repair SHALL occur
- **AND** garbage collection behavior SHALL be identical to a build without this feature

#### Scenario: Enabling at runtime
- **WHEN** the user sets the inline-repair threshold to a non-zero value via `FT.CONFIG SET`
- **THEN** subsequent writes SHALL be eligible for inline repair
- **AND** no restart SHALL be required

### Requirement: Inline repair is reported in index statistics

`FT.INFO` SHALL report the number of inline repairs performed and the net bytes they
reclaimed, as `inline_gc_repairs` and `inline_gc_bytes_collected`. The same values SHALL
appear in the module's `INFO` section.

These are per-spec counters reported alongside `total_inverted_index_blocks`, not inside the
`gc_stats` section: `gc_stats` is rendered from the fork GC's own context, which has no access
to these, and keeping them separate lets the two reclaim paths be compared rather than summed.

#### Scenario: Counters advance
- **WHEN** inline repair is enabled and at least one repair has occurred on an index
- **THEN** `FT.INFO` on that index SHALL report a non-zero `inline_gc_repairs`
- **AND** a non-zero `inline_gc_bytes_collected`

#### Scenario: Counters stay at zero while disabled
- **WHEN** inline repair is disabled
- **THEN** both counters SHALL remain 0 regardless of indexing and deletion activity

## MODIFIED Requirements

### Requirement: The fork GC does not repair the last block

This existing behavior is unchanged, but its consequence changes: with inline repair enabled,
the last block is covered by the write path instead of being left indefinitely.

The fork GC SHALL continue to discard any scan delta referring to the last block when that
block changed between the scan and the apply, and SHALL continue to report this as
`gc_blocks_denied`.

#### Scenario: Inline repair changes the tail during a fork-GC cycle
- **WHEN** a fork-GC child has scanned an index
- **AND** an inline repair modifies that index's last block before the parent applies the delta
- **THEN** the parent SHALL discard the delta entry for the last block
- **AND** the remaining blocks SHALL still be repaired
- **AND** the index SHALL remain correct
