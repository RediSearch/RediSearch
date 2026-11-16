# Add advisory iterator match estimates

## Why

Query iterators expose an upper bound through `NumEstimated` / `num_estimated`.
That bound is deliberately conservative: an intersection uses the smallest
child bound, a union adds child bounds, and a partial numeric range can report
the count of the entire underlying range. It does not express the expected
number of matching documents. Overlap and partial ranges can make the gap
between the bound and the actual count substantial.

Changing the existing method into a statistical estimate would violate its
contract and could affect consumers that use a zero bound to rule out work.
An independent advisory API lets callers distinguish these two meanings.

## What changes

- Every active query iterator exposes a best-effort estimate of its total
  matching documents, alongside the existing upper bound.
- Compound iterators compose child estimates using a shared document
  population. Numeric filters use available range statistics where meaningful.
- C and Rust expose equivalent methods. Wrappers forward the estimate without
  changing its meaning.
- Existing query planning, result production, command replies, and persisted
  formats retain their current behavior. This PR makes the new information
  available internally; selecting consumers is a separate change.
- Tests cover the estimate contract and approximation boundaries. Repository
  benchmarks measure existing iteration overhead and the cost of explicitly
  requesting an estimate.

This draft does not add sampling, joint-distribution statistics, or per-read
match counters. Those require additional evidence and separate cost decisions.
