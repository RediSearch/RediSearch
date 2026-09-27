# Spec-lock ownership

[MOD-17024](https://redislabs.atlassian.net/browse/MOD-17024) supersedes the
shared-context storage decision in section 3.5 of the query-lifetime design
([closed PR #9426](https://github.com/RediSearch/RediSearch/pull/9426)).

The owning thread records the spec and read/write mode in TLS. A request or
search context can move between workers without moving pthread lock ownership.
Each acquisition must finish on the acquiring thread. Unlock with an empty
thread slot is idempotent and cannot unlock another thread's rwlock. Acquiring
while the slot is occupied, or unlocking a different spec, is an invariant
violation. Read acquisition and release pair dictionary rehash pauses.

Context destruction does not unlock. Cursor reservation can reap an unrelated
idle request while the reserving thread holds a lock for its active request.
Acquisition scopes must therefore release explicitly, including plan-build and
cursor-reservation failures and EXPLAIN completion.

The caller audit found no production path that needs two simultaneously held
spec locks. During synchronous hybrid depletion, subqueries run on the owning
thread and their EOF/error cleanup must not release the outer scope's read lock.
`IndexSpec_SuppressUnlock` makes those unlock calls no-ops.
`IndexSpec_AllowUnlock` ends suppression without releasing the lock, allowing
the outer scope to unlock explicitly. This scope cannot nest or propagate to
another worker, and it never reacquires the writer-preferring rwlock.

| Caller group | Ownership and nesting |
| --- | --- |
| Synchronous search, aggregate, explain, cursors | The command acquires; iterator completion, loader transition, cursor-cycle completion, or an explicit error path releases on that same thread. Parked cursors own no lock. |
| Worker search and cursor reads | Each worker cycle starts and ends with an empty TLS slot. The safe loader releases before taking the Redis lock. Main-thread request cleanup cannot release a worker's lock. |
| Synchronous hybrid | Build owns one read lock. In-memory depletion suppresses subquery unlocks on the same thread; only the outer scope releases. Disk depletion releases after snapshot construction. Early build errors never enable suppression. |
| Background hybrid | Build and each safe depleter acquire independent read locks. The launcher releases its own lock after all depleters acquired or skipped. Each depleter releases on its worker before signaling completion. Failed try-locks install no ownership. |
| Indexing, scanner, document metadata, notifications | Each per-spec update acquires and releases in one invocation. Multi-index loops release before advancing. Field-expiration reindexing drops its lock before entering the indexing path. Indexer yields occur before acquisition. |
| GC | Tag delta application and Rust numeric/term write guards acquire and release around each update. Existing C write-lock FFI entrypoints delegate to the TLS APIs. Disk compaction begin/end callbacks retain the same-thread contract. |
| Module commands, vector cleanup, INFO, RDB | Per-spec scopes release before moving to another spec. RDB keeps its existing conditional locking policy. Normal INFO uses the same read API, including dictionary rehash pausing. Crash reports omit aggregate collection, which could reacquire interrupted spec/vector locks, and retain the lock-free current-index snapshot. |

The proposed future optimization of holding a lock across cursor reads is
removed: a parked cursor may resume on another worker. This supersedes section
3.4's future-improvement note and section 8's risk 6 in the closed design PR.
The historical branch is not reopened or rewritten.

Concurrency coverage includes independent readers sharing a context, failed
try-locks, cleanup from a non-owner thread, synchronous unlock suppression, and
two actual safe depleters held concurrently behind a deterministic gate. The
latter exercises the hybrid lock handoff with a shared search context.

This is a master-only internal refactor. A release-branch correctness fix must
be assessed independently against that branch's ownership and hybrid code;
this change does not imply a backport label.
