# `MRConn` ownership clarification — proposal

Scope: `src/coord/rmr/conn.c` / `conn.h`. The `MRConnState` machine is clean;
the goal here is to make the `redisAsyncContext *` ("ac") ownership model
equally explicit and to tighten the contract between `MRConnState` transitions
and ac lifetime.

## Problem

The ac goes through distinct lifetime phases that are not named anywhere in
code. Today the phase is reconstructed by the reader from (a) whether we're
inside a hiredis callback, (b) whether `redisLibuvAttach` has been called, and
(c) whether `conn->conn == NULL`. The three detach helpers
(`detachCbData` / `detachAndFreeAc` / `detachAndDisconnectAc`) exist
*because* of this variance, but the phase itself is implicit.

```
              AsyncConnect()                 LibuvAttach OK              (teardown callback entered)
  ─────▶ [ Phase 0: none ] ──────▶ [ Phase 1: we own ] ──────▶ [ Phase 2: borrowed by hiredis ] ──────▶ [ Phase 3: hiredis is freeing ]
```

Every `MRConn_SwitchState(conn, MRConn_Reconnecting)` call site is preceded by
an ad-hoc detach whose variance is purely a function of which phase the ac is
in at that point.

## Proposal

Three layers, applied together in one small PR local to `conn.c` (no public
API change).

### Layer A — name the ac phase as data on `MRConn`

```c
typedef enum __attribute__((packed)) {
  AcPhase_None,              // no ac attached
  AcPhase_OwnedLocally,      // between AsyncConnect and LibuvAttach, or reclaimed after enqueue failure
  AcPhase_BorrowedByHiredis, // attached; hiredis drives teardown
} AcPhase;

struct MRConn {
  …
  redisAsyncContext *conn;
  AcPhase            acPhase;
};
```

Transitions:

- After `redisAsyncConnectWithOptions` → `AcPhase_OwnedLocally`.
- After `redisLibuvAttach` success → `AcPhase_BorrowedByHiredis`.
- After any detach → `AcPhase_None`.
- Phase 3 (hiredis-is-freeing) stays stack-local: asserted at the top of the
  three hiredis callbacks, never stored.

### Layer B — collapse the detach helpers into one ownership-driven op

Replace `detachCbData` / `detachAndFreeAc` / `detachAndDisconnectAc` with:

```c
typedef enum {
  AcRelease_LocalTeardown,    // we must redisAsyncFree (or no-op if None)
  AcRelease_GracefulShutdown, // redisAsyncDisconnect if Borrowed, else LocalTeardown
  AcRelease_HiredisIsFreeing, // sever back-ptr only; must be Borrowed on entry
} AcReleaseReason;

static void MRConn_ReleaseAc(MRConn *conn, AcReleaseReason reason);
```

Dispatch is driven by `(acPhase, reason)`. Illegal combinations (e.g.
`HiredisIsFreeing` with `AcPhase_OwnedLocally`) assert rather than miscompile.
Call-site intent stays visible via `reason`; the mechanics live in one place.

Renaming suggestion (intent, not mechanics): `acHandoverToHiredis`,
`acTearDownLocally`, `acRequestGracefulShutdown` as thin wrappers if desired.

### Layer C — document and assert the `(MRConnState × AcPhase)` invariant

```
State            | AcPhase on entry        | AcPhase on exit
-----------------+-------------------------+-------------------------------------------
Connecting       | None                    | BorrowedByHiredis (OK) or None (→ Reconnecting)
Authenticating   | BorrowedByHiredis       | BorrowedByHiredis (OK) or None (→ Reconnecting)
Connected        | BorrowedByHiredis       | BorrowedByHiredis
ReAuth           | BorrowedByHiredis       | BorrowedByHiredis
Reconnecting     | None                    | None    ← invariant that replaces 7 ad-hoc detaches
Freeing          | any                     | None    ← terminal; conn handed to freeConn
```

`MRConn_SwitchState(..., Reconnecting)` becomes
`RS_ASSERT(conn->acPhase == AcPhase_None)` on entry — nothing else. Every
caller that previously did a bespoke detach now calls `MRConn_ReleaseAc(..)`
with the appropriate reason *before* the transition; the state machine itself
stays focused on timer/IO scheduling.

## Consequences

- The "seven different detach-then-Reconnect" sites reduce to one pattern:
  `MRConn_ReleaseAc(conn, reason); MRConn_SwitchState(conn, MRConn_Reconnecting);`.
- `MRConn_Reconnecting` remains a single state. The per-site variance moves
  from *state* to *transition edge*, which is where it belongs.
- New contributors have an invariant table to check against instead of
  reverse-engineering ac lifetime from the call stack.

## Smaller companion cleanups

- Move the current per-helper comments into a single "redisAsyncContext
  ownership" block at the top of `conn.c` with the phase diagram.
- Add a one-liner on `MRConn` documenting that `freeConn` is asynchronous via
  the inlined timer's `uv_close`.
- Add a one-liner on `MRConnPool` stating that lifetime is owned by the dict
  via `MRConnPool_Free`.

## Non-goals

- **Do not** refcount `MRConn`. The "pool owns, uv defers the free" model is
  fine; refcounts would obscure that hiredis holds only a borrowed back-pointer
  cleared on detach.
- **Do not** split `MRConn_Reconnecting` into variants. The variance is in the
  release edge, not the destination state.
- **Do not** try to store Phase 3 as data. It is genuinely stack-local;
  asserting it at callback entry is cleaner than tracking it.

## Suggested rollout

1. Layer A alone (add `AcPhase`, set/clear it at the existing transition
   points, assert invariants). No behavioral change; purely additive.
2. Layer B (collapse the detach helpers) once A's assertions have run in CI.
3. Layer C (add the invariant table + `SwitchState` assertions) to lock in
   the contract.
