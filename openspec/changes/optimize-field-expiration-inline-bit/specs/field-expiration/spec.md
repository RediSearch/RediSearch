# field-expiration (delta)

Behavior delta for honoring field-level expirations (HFE) during query
iteration. On merge, fold into `openspec/specs/field-expiration/spec.md`.

These requirements pin **observable behavior**. The inline per-entry bit is an
internal optimization: query results must be identical to consulting the TTL
table directly, and the only externally visible change is that field-TTL
*presence* transitions reindex the affected document.

## MODIFIED Requirements

### Requirement: Queries honor field-level expiration
A query that filters by a field SHALL exclude a document whose matched field has
an expiration in the past, and include it otherwise, regardless of how many
documents in the index carry field expirations.

#### Scenario: Document without any field TTL is never excluded as expired
- **WHEN** an index contains documents both with and without field-level expirations
- **AND** a document `d` has no field-level expiration on any field
- **AND** a query matches `d`
- **THEN** `d` SHALL be returned (it SHALL NOT be treated as expired)

#### Scenario: Document with an expired matched field is excluded
- **WHEN** a document `d` has a field-level expiration in the past on the matched field
- **AND** a query matches `d` on that field
- **THEN** `d` SHALL NOT be returned

#### Scenario: Document with a not-yet-expired matched field is included
- **WHEN** a document `d` has a field-level expiration in the future on the matched field
- **AND** a query matches `d` on that field
- **THEN** `d` SHALL be returned

#### Scenario: Parity across index field types
- **WHEN** the matched field is a TEXT, TAG, or NUMERIC field with a field-level expiration
- **THEN** expiration filtering SHALL produce the same inclusion/exclusion result for each field type

### Requirement: Field-TTL presence is reflected in subsequent queries
A change to whether a document has *any* field-level expiration SHALL be
reflected in subsequent query results, including via the document reindex that
such a presence change triggers.

#### Scenario: Adding a first field TTL takes effect
- **WHEN** a document `d` initially has no field-level expiration and is matched by a query
- **AND** `HEXPIRE` then sets a future expiration on a matched field of `d`
- **AND** that expiration later passes
- **THEN** a subsequent query SHALL exclude `d` as expired

#### Scenario: Removing the last field TTL takes effect
- **WHEN** a document `d` has a single field-level expiration
- **AND** `HPERSIST` removes that expiration (or it expires and is cleared)
- **THEN** a subsequent query matching `d` SHALL return `d` and SHALL NOT treat the field as expiring

### Requirement: Field-expiration state survives index rebuild
After the index is rebuilt from the keyspace (e.g. on load), expiration
filtering SHALL behave identically to before the rebuild.

#### Scenario: Reload preserves expiration behavior
- **WHEN** an index with a mix of TTL'd and non-TTL'd documents is rebuilt from the keyspace
- **THEN** queries SHALL apply field expiration with the same results as before the rebuild
