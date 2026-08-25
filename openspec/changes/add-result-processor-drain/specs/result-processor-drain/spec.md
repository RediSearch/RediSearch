## ADDED Requirements

### Requirement: ResultProcessor exposes a result-yielding Drain operation

Every ResultProcessor SHALL expose a Drain operation that accepts caller-owned
`SearchResult` storage and returns one of RESULT, EOF, or ERROR.

- RESULT SHALL mean the output contains a valid result owned by the caller.
- EOF SHALL mean the drain sequence is terminal.
- ERROR SHALL mean result production failed and the drain sequence is terminal.
- Drain SHALL NOT return timeout, paused, depleting, or pending.

#### Scenario: Drain yields a result

- **GIVEN** a processor owns a result eligible for the drain path
- **WHEN** its Drain operation returns RESULT
- **THEN** the caller owns a `SearchResult` initialized under the same convention as `Next`
- **AND** the processor retains no ownership that would cause duplicate release

#### Scenario: Drain reaches EOF

- **GIVEN** a processor has returned drain EOF
- **WHEN** Drain is called again
- **THEN** it returns EOF without producing another result

#### Scenario: Drain encounters an execution failure

- **GIVEN** a processor cannot produce the next serializable result because expression evaluation, an upstream, or another execution step failed
- **WHEN** Drain observes that failure
- **THEN** it returns ERROR rather than disguising the failure as EOF

### Requirement: Drained results preserve pipeline output semantics

Drain SHALL continue the current pipeline output sequence. It SHALL NOT restart
the processor or re-yield results already transferred through `Next`.

A result returned from the pipeline entry through Drain SHALL be suitable for
normal result serialization. Each processor SHALL apply the transformation,
filtering, loading, paging, partial finalization, or fan-in semantics required at
its output boundary.

#### Scenario: Streaming transformation

- **GIVEN** a streaming processor normally transforms an upstream result
- **WHEN** upstream Drain yields a result
- **THEN** the processor applies the same externally visible transformation before returning RESULT

#### Scenario: Buffering processor times out during accumulation

- **GIVEN** a sorter, normalizer, or grouper has accumulated a partial state
- **WHEN** Drain claims that processor
- **THEN** the processor stops accumulation
- **AND** finalizes and yields the valid partial output without pulling more work through upstream `Next`

#### Scenario: Async loader has incomplete work

- **GIVEN** an async loader has ready rows and rows requiring unfinished I/O
- **WHEN** Drain runs
- **THEN** it may yield the complete ready rows
- **AND** it omits or cancels incomplete rows rather than waiting or returning an unserializable result

### Requirement: Drain is safe alongside one in-flight Next chain

Drain SHALL be thread-safe with at most one concurrent call active anywhere in
the `Next` chain. Pipeline and processor lifetime SHALL be guaranteed externally
until both calls have completed. Multiple concurrent `Next` callers and multiple
concurrent Drain callers are outside the contract.

#### Scenario: Next owns a result before the handoff

- **GIVEN** the in-flight `Next` call claimed a result before the drain handoff
- **WHEN** Drain begins
- **THEN** the result remains owned by the `Next` path
- **AND** Drain does not duplicate or steal it
- **AND** omitting that result from the partial timeout reply is permitted

#### Scenario: Drain claims buffered state

- **GIVEN** a result remains buffered and unclaimed at the handoff
- **WHEN** Drain claims the processor state
- **THEN** the result is eligible only for the Drain path

#### Scenario: Late Next completion

- **GIVEN** an in-flight `Next` call completes after the handoff
- **WHEN** its result is excluded from the timeout reply
- **THEN** that result is nevertheless destroyed exactly once
- **AND** its late bookkeeping cannot overwrite Drain-owned reply state

### Requirement: Drain does not depend on background progress

Drain SHALL NOT wait for the in-flight `Next` call, scheduled background work,
blocking I/O, a condition variable, or a global interpreter/runtime lock. It MAY
take short processor-local critical sections under an assumption of low
contention.

#### Scenario: Background job has not started

- **GIVEN** a processor's background job is queued but has not been scheduled
- **WHEN** Drain runs
- **THEN** Drain claims ready state or terminates without waiting for the job

#### Scenario: Next is blocked while the drainer owns the GIL

- **GIVEN** the `Next` thread is waiting for the GIL held by the drain caller
- **WHEN** Drain traverses the processor
- **THEN** Drain does not wait for `Next` to release processor-wide state

### Requirement: Drain establishes terminal producer handoff

Once a processor returns drain EOF or ERROR, no background completion or later
state transition SHALL make another result visible through Drain. The
orchestration layer SHALL prevent a new `Next` call from starting after the
handoff; behavior for such a new call need not be enforced by every processor.

#### Scenario: Async completion after EOF

- **GIVEN** Drain returned EOF while an async operation was outstanding
- **WHEN** the operation completes later
- **THEN** it does not publish a new drain result
- **AND** its resources are released exactly once

### Requirement: Drain does not introduce query-level synchronization

Drain SHALL avoid mutable `QueryProcessingCtx` bookkeeping, including
`resultLimit`, result counters, and shared error publication. Its caller SHALL
own the private result budget, reply serialization, and timeout publication.

The orchestration layer SHALL reuse the blocked-client and `QueryRequest`
lifetime rules. MOD-17482 SHALL NOT add an independent query-level handoff or
lifetime protocol.

#### Scenario: Drain enforces a private reply budget

- **GIVEN** the timeout reply has a caller-owned result budget
- **WHEN** Drain yields a result
- **THEN** the caller decrements that private budget
- **AND** Drain does not concurrently mutate `QueryProcessingCtx.resultLimit`

#### Scenario: Query lifetime spans concurrent entry

- **GIVEN** one `Next` chain and one Drain call are active
- **WHEN** their execution overlaps
- **THEN** the blocked-client and `QueryRequest` owner keeps the pipeline alive
- **AND** no ResultProcessor adds a second query-lifetime protocol

### Requirement: The Rust API represents the same concurrency contract

The Rust ResultProcessor abstraction SHALL expose Drain as a first-class trait
operation. Concurrent FFI entry into `Next` and Drain SHALL NOT create
overlapping mutable Rust references to the processor or header. Concrete Rust
processor state SHALL use fine-grained interior synchronization rather than a
wrapper lock held across a complete call.

#### Scenario: Rust Next and Drain run concurrently

- **GIVEN** a Rust ResultProcessor is entered through `Next` and Drain on separate threads
- **WHEN** the two operations access disjoint or synchronized local state
- **THEN** the FFI wrapper does not create aliasing that violates Rust's reference rules
- **AND** Drain remains able to complete while `Next` is blocked upstream

### Requirement: Processor implementations remain in their current language

The Drain implementation stack SHALL NOT migrate an additional C
ResultProcessor to Rust. The shared API SHALL nevertheless permit a later Rust
migration without changing the Drain contract or retaining a C-only adapter.

#### Scenario: Existing Rust processor

- **GIVEN** the counter is already implemented in Rust
- **WHEN** the Drain API is added
- **THEN** it implements the same C-visible contract through the Rust trait and FFI wrapper

#### Scenario: Existing C processor

- **GIVEN** a processor is currently implemented in C
- **WHEN** its Drain PR lands
- **THEN** it remains implemented in C

### Requirement: MOD-17482 integration preserves current timeout behavior

The controlled-path integration in this change SHALL exercise Drain for AREQ
and HybridRequest pipelines without removing the current background-exit wait or
changing timeout reply policy. The later timeout-flip change may use the same API
without those waits.

#### Scenario: Controlled integration

- **GIVEN** an existing timeout test request
- **WHEN** the MOD-17482 integration path invokes Drain
- **THEN** the serialized reply and cursor behavior remain unchanged
