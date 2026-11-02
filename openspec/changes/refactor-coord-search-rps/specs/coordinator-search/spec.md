# Coordinator search compatibility requirements

These are the final refactor acceptance requirements, not new promises about
historical timeout result completeness. The behavior ledger owns unresolved
classifications and evidence.

## ADDED Requirements

### Requirement: Preserve completed search behavior

For identical accepted shard replies and request options, the replacement
reducer SHALL preserve successful reply values, types, structure, ordering,
pagination and hit counts unless a separate behavior change is approved.

#### Scenario: KNN followed by another ordering

- **WHEN** the existing plan selects global nearest K before final ordering
- **THEN** the replacement SHALL apply membership selection before that ordering
- **AND** pagination SHALL NOT replace hit-count computation.

#### Scenario: Distance sorting

- **WHEN** a non-count-only KNN query sorts by distance descending
- **THEN** the replacement SHALL select global nearest K before descending order.
- **WHEN** distance sorting is ascending or the query is count-only
- **THEN** the existing shortcut and path-specific count semantics SHALL remain.

#### Scenario: Remote output values

- **WHEN** a result carries payload, score explanation or nested returned values
- **THEN** the replacement SHALL preserve their wire types and precision
- **AND** backing data SHALL remain valid until all consumers release it.

### Requirement: Preserve lifecycle during the mechanical migration

The initial migration SHALL retain existing timeout ownership and fanout behavior.
Every request SHALL complete and release its owned resources exactly once.

#### Scenario: Timeout overlaps reduction

- **WHEN** timeout overlaps background reduction
- **THEN** main and BG SHALL NOT mutate the same pipeline state concurrently
- **AND** timeout notification alone SHALL NOT authorize freeing borrowed state.

### Requirement: Account for observable differences

Every intentional observable difference SHALL have a ledger entry with its
trigger, before/after behavior, classification and supporting evidence.

#### Scenario: Uncertain compatibility

- **WHEN** a difference lacks sufficient contract or runtime evidence
- **THEN** it SHALL remain unclassified
- **AND** it SHALL NOT be accepted as soft solely because documentation is absent.

## Approved corrections

Nearest-K selection SHALL use vector distance independently of a numeric final
SORTBY field. Distance DESC SHALL order the global nearest-K set rather than
selecting the farthest shard candidates. The distance ASC and count-only shortcuts
remain. Standard typed field comparison replaces encoded-prefix comparison;
document-key ties and absent-value placement remain explicit compatibility rules.

### Requirement: Serialize borrowed rows from source-owned replies

The source RP SHALL own accepted shard replies until request cleanup. Candidates
SHALL borrow their output values and serialize selected rows directly, without
converting returned fields, payloads or explanations into intermediate RSValues.
Stored candidates and downstream processors SHALL be freed before their source.
Results and profiles SHALL use the same acquired publication snapshot; later
publications SHALL remain owned for final cleanup.

### Requirement: Report encountered parse errors immediately

An encountered malformed row or required envelope field SHALL stop reduction and
set QueryError. This error SHALL take precedence over partial results, including
RETURN/RETURN-STRICT and tolerated shard timeout/OOM. This approved error-precedence
change does not change successful reply representations.

### Integration boundary

Strict cancellation remains at shard boundaries. Shared reply-buffer serialization,
per-row cancellation, public Drain continuation and nonblocking BG/main handoff
remain integration work for the separate reply-buffer/Drain stack.
