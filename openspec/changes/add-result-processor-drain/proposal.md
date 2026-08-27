# Add a result-processor drain API

## Why

The non-blocking timeout design needs a generic way for the timeout thread to
deplete an active result-processor pipeline while the query thread may still be
executing `Next`. Current timeout cleanup depends on request-specific pipeline
knowledge and direct access to processor internals, which prevents a common
timeout path for aggregate and hybrid requests.

ResultProcessor migrations to Rust are also blocked on this API change. The
shared contract and Rust wrapper must support concurrent `Next` and Drain before
additional processors can be ported without immediately redesigning their
interface.

This change is the ResultProcessor preparation tracked by MOD-17482. It does
not itself flip timeout policy behavior, remove the existing background-exit
wait, or change reply storage.

## What Changes

- Add a thread-safe, result-yielding drain operation to the ResultProcessor API.
- Make Drain an alternate pipeline execution path whose yielded results can be
  serialized for return-on-timeout and return-strict behavior.
- Support one drain caller concurrent with at most one thread executing the
  `Next` chain. Pipeline lifetime is guaranteed externally.
- Give every ResultProcessor type an explicit drain implementation or an
  explicit choice of the default behavior.
- Preserve single ownership of yielded results and exactly-once cleanup.
- Keep synchronization short and processor-local; Drain never waits for the
  query thread or for background work to make progress.
- Make Drain a first-class operation in the Rust ResultProcessor abstraction so
  a processor can migrate without retaining a C-specific drain shim.
- Exercise the API through an existing synchronized cleanup or fault-injection
  path without changing timeout replies.

### Non-goals

- Migrating any additional ResultProcessor implementation from C to Rust.
- Publishing the active pipeline entry through `QueryRequest`.
- Removing waits from timeout callbacks.
- Changing reply-array synchronization or timeout policy semantics.
- Supporting multiple concurrent `Next` or drain callers.
