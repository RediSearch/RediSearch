# iterator-match-estimates (delta)

## ADDED Requirements

### Requirement: Distinct advisory estimate

Every active query iterator SHALL expose an advisory prediction of its total
matching documents. The existing upper-bound method SHALL retain its contract.
The advisory result SHALL be finite and nonnegative, capped by the supplied
population and the existing upper bound. It MAY underestimate or overestimate
the actual number of matches.

#### Scenario: Empty population

- **WHEN** the caller supplies a document population of zero
- **THEN** the advisory estimate SHALL be zero
- **AND** the estimator SHALL avoid division by zero.

#### Scenario: Unavailable distribution statistics

- **WHEN** an iterator has no more informative applicable model
- **THEN** it SHALL fall back to its existing upper bound capped to the population.

#### Scenario: Advisory zero does not establish emptiness

- **WHEN** an advisory estimate is zero but the iterator can yield results
- **THEN** ordinary iterator operations SHALL continue to yield those results.

### Requirement: Shared population for compound estimates

Boolean composition SHALL use the same caller-supplied population for all
children. Intersections and unions SHALL expose their independence-based
predictions separately from their conservative upper bounds.

#### Scenario: Independent conjunction

- **WHEN** a population is 1,000 documents
- **AND** two children predict 100 and 200 matches
- **THEN** their ordinary conjunction SHALL predict 20 matches.

#### Scenario: Independent disjunction

- **WHEN** a population is 1,000 documents
- **AND** two children predict 100 and 200 matches
- **THEN** their ordinary disjunction SHALL predict 280 matches, subject to
  floating-point rounding.

#### Scenario: Correlated predicates

- **WHEN** both conjunction children match the same 100 documents out of 1,000
- **THEN** the independence model MAY predict fewer than the actual 100 matches
- **AND** the original upper bound and yielded results SHALL remain unchanged.

### Requirement: Total-estimate lifecycle

Reading the advisory estimate SHALL NOT consume or reposition an iterator.
The result SHALL describe total matches rather than matches remaining after
the current document. Index changes may change the available statistics.

#### Scenario: Read or seek before estimation

- **WHEN** an iterator advances while its underlying data and population stay fixed
- **THEN** requesting the estimate SHALL NOT subtract visited documents solely
  because they have been visited
- **AND** the next iterator operation SHALL behave as it would without that request.

### Requirement: Interop consistency

The C callback and the Rust trait method SHALL use equivalent population and
result semantics. Wrappers and iterator variants SHALL preserve those semantics
when forwarding calls.

#### Scenario: Rust iterator called through C

- **WHEN** a Rust iterator is wrapped in the C iterator interface
- **AND** the same population is supplied
- **THEN** its C callback SHALL return the same advisory estimate as the Rust method.

### Requirement: Preserve existing execution and persistence

This change SHALL NOT replace existing planning inputs, alter result pruning,
introduce per-result sampling or accounting, change command reply shapes, or
change serialization formats.

#### Scenario: Existing query execution

- **WHEN** an existing query runs without requesting the new advisory estimate
- **THEN** its existing planning decisions and result semantics SHALL remain unchanged.
