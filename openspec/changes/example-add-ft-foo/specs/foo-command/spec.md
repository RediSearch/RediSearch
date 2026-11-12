# foo-command (example delta)

> Illustrative example — `FT.FOO` is fictional. On merge, the requirements below
> would be folded into `openspec/specs/foo-command/spec.md`.

## ADDED Requirements

### Requirement: FT.FOO summarizes an index
The `FT.FOO` command SHALL return a single human-readable summary of an existing
index, derived from data the index already maintains, without modifying any
state.

#### Scenario: Summary of a populated index
- **WHEN** an index `idx` exists with 3 indexed documents and 2 schema fields
- **AND** the user executes `FT.FOO idx`
- **THEN** the reply SHALL be a single string reporting the index name, a document count of `3`, and a field count of `2`

#### Scenario: Summary of an empty index
- **WHEN** an index `idx` exists with a schema but no indexed documents
- **AND** the user executes `FT.FOO idx`
- **THEN** the reply SHALL report a document count of `0`
- **AND** the command SHALL NOT return an error

#### Scenario: Unknown index
- **WHEN** the user executes `FT.FOO missing` and no index named `missing` exists
- **THEN** the command SHALL return an error `Unknown index name (or name is an alias itself)`

#### Scenario: Alias argument
- **WHEN** `alias` is an alias for an existing index `idx`
- **AND** the user executes `FT.FOO alias`
- **THEN** the reply SHALL summarize `idx`
