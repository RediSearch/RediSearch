# FT.CURSOR READ + RETURN_STRICT — "resend after timeout" race investigation

> **Status: CONFIRMED.** Introduced by **PR #9796 (MOD-15285)** — *"Support
> RETURN_STRICT for Shard-level cursor reads"*, open on branch
> `codex/MOD-15285-return-strict-hybrid-cursors`. That PR extends RETURN_STRICT
> to the shard/standalone `FT.CURSOR READ` path and, by design, "stored
> strict-timeout replies keep the cursor id … so clients can retry" — but the
> worker parks the cursor *after* the timeout callback replies with that id,
> opening the resend window (§3b/§4).
> The coordinator paths from **PR #9942 (MOD-14075)** and #9451 on `origin/master`
> are *not* affected (§3a).

## 1. Reported edge case

For `FT.CURSOR READ` under `RETURN_STRICT`:

1. The read times out. The **timeout callback** (main thread) replies an
   empty, cursor-shaped result **plus the cursor id**.
2. The client immediately re-sends `FT.CURSOR READ` with the same cid.
3. The re-send reaches the cursor table **before the worker thread has paused /
   re-registered the cursor**, so the take fails and the client gets a *false*
   `"Cursor not found"`.

**This reproduces on the shard/standalone RETURN_STRICT path added in
branch `codex/MOD-15285-return-strict-hybrid-cursors`** (see §3b). It does *not*
reproduce on the coordinator path on master (§3a). The fix needs a small
refactor in cursor management.

## 2. Relevant code paths

All `aggregate_exec.c` line numbers below are on branch
`codex/MOD-15285-return-strict-hybrid-cursors`.

| Concern | Location |
|---|---|
| Read entry; **takes the cursor on the main thread** | `RSCursorReadCommand` → `Cursors_TakeForExecution` (`:2249`) |
| Shard/standalone RETURN_STRICT arm (block + dispatch worker with already-taken cursor) | `RSCursorReadCommand` (`:2272-2298`) |
| Worker entry | `cursorRead_ctx` → `cursorRead` → `runCursor` (`:2128`, `:1982`) |
| Worker stashes + parks/frees cursor | `runCursor` stash (`:2014`), claim-lost park/free (`:2020-2032`) |
| **Shard/standalone RETURN_STRICT timeout callback** | `CursorReadTimeoutReturnStrictCallback` (`:1735`) |
| Coord RETURN_STRICT take+publish (master, **safe**) | `coordCursorReadReturnStrict` (`:2162`) |
| Coord timeout callback (master, **safe**) | `dist_aggregate.c` `DistCursorReadTimeoutReturnStrictClient` (`:847`) |
| Take semantics | `Cursors_TakeForExecution` (`cursor.c`): returns `NULL` if `pos==-1` (**busy**) **or** absent |
| Lookup vs idle | `Cursors_TakeForExecution` removes from **idle list only**; the cursor stays in `cl->lookup` until `Cursor_Free`. `Cursor_Pause` re-adds to idle. |

## 3. Two read paths behave differently

The distinguishing question is **which thread takes the cursor, and which thread
disposes it.** `Cursors_TakeForExecution` makes the cursor *busy* (`pos==-1`,
removed from the idle list); only `Cursor_Pause`/`Cursor_Free` makes it
takeable/gone again. A re-send fails with "Cursor not found" iff its
`Cursors_TakeForExecution` runs while the cursor is still busy.

### 3a. Coordinator path on master (`coordCursorReadReturnStrict`) — SAFE

Here the **background thread** takes the cursor, under `setRequestLock`, and the
**main-thread timeout callback** disposes it:

- **(I1) take/publish and timeout are serialized by `setRequestLock`.** The
  timer (`DistCursorReadTimeoutReturnStrictClient`) does `SetTimedOut` → read
  `req` under the lock; the BG does `if TimedOut return` → take → `SetRequest`
  under the same lock. So the empty+cid reply (`req==NULL`) is produced **only
  when the cursor was never taken** — the BG then sees `TimedOut` and bails
  without taking, leaving it idle.
- **(I2) when the BG did take, the timer disposes it synchronously on the main
  thread before returning** — `AREQ_WaitForAggregateResultsComplete` →
  `AREQ_ReplyWithStoredResults`, whose `Cursor_Pause`/`Cursor_Free` is at
  `:1645-1652`. The timeout reply is a true main-thread reply flushed in the
  *same* `beforeSleep`, but always *after* the callback (hence after disposal)
  returns. So a re-send is always parsed after the cursor is idle/freed.

> An earlier draft justified I2 by claiming the reply "isn't flushed until the
> BG's `RM_UnblockClient`". Per `redis/blocked_client_timeout_reply.md` that is
> wrong (the timeout reply flushes on the main thread in the same `beforeSleep`,
> independent of the BG). The coord path is still safe, because disposal is
> **main-thread-synchronous inside the callback** — not because the flush waits
> on the BG.

### 3b. Shard/standalone RETURN_STRICT path on the branch — BROKEN (confirmed)

This is the path added in `codex/MOD-15285-return-strict-hybrid-cursors`, and it
breaks the I2 assumption. Here the **main thread takes the cursor** up front and
the **worker thread disposes it**:

1. `RSCursorReadCommand` calls `Cursors_TakeForExecution(cid)` on the **main
   thread** (`:2249`) → cursor is now **busy**. It then blocks the client, arms
   `CursorReadTimeoutReturnStrictCallback`, and dispatches `cursorRead_ctx` to a
   worker with the **already-busy** cursor (`:2272-2298`).
2. The worker runs `runCursor`; only **after** `sendChunk`, if it lost the sync
   claim (`aggregateResultsClaimLost`), does it park/free the cursor —
   `Cursor_Pause` for a follow-up READ (`:2020-2032`).
3. The timeout callback's **claim-wins** branch replies and returns immediately,
   **without disposing the cursor**:

   ```c
   if (AREQ_TryClaimAggregateResults(req)) {
     // ... "when the worker observes the failed claim, it parks the
     //      already-taken cursor for the advertised id."
     return cursor_read_empty_reply_timeout(ctx, node->cursorId, IsInternal(req));
   }
   ```
   (`CursorReadTimeoutReturnStrictCallback`, `:1749-1753`). Unlike the
   claim-*lost* branch just below it, this branch does **not**
   `AREQ_WaitForAggregateResultsComplete` and does **not** dispose the cursor —
   it relies on the worker to park it *later*.

There is **no happens-before edge** between the worker's `Cursor_Pause` (step 2)
and the timeout reply's flush (step 3). The timer wins the claim precisely when
the worker has *not yet* reached the aggregation phase, so at flush time the
worker is still mid-`sendChunk`, far from the park. The comment at
`cursorRead_ctx` ("…park/free the cursor before the timeout callback replies")
is therefore **incorrect** for the claim-wins ordering.

## 4. The confirmed interleaving (3b)

```
main thread (read#1)                worker (read#1)            client
--------------------------------    --------------------       ----------------
TakeForExecution(cid)  → BUSY
block + dispatch worker
                                    runCursor: stash cursor
  ── timer fires (beforeSleep) ──   (still pre-aggregation)
  CursorReadTimeoutReturnStrict:
    SetTimedOut
    TryClaim → WINS
    reply empty + cid (NO dispose)
  handleClientsWithPendingWrites →  ........................→  receives reply
                                                              re-sends READ cid
  ── read#2 (next iteration) ──
  RSCursorReadCommand:
    TakeForExecution(cid)
      → cursor STILL BUSY (pos==-1)
      → NULL → "Cursor not found"   sendChunk returns
                                    aggregateResultsClaimLost
                                    Cursor_Pause(cursor)  ← too late
```

The window is **wide**, not a tight race: the timer wins the claim only when the
worker is still before the aggregation phase, so the cursor stays busy for the
whole duration of the worker's `sendChunk`.

**Conclusion: the §1 race is real on the 3b path.** It is *not* present on the
coord path (3a), which is why a master-only analysis (my earlier draft) wrongly
concluded "no race".

## 5. Root cause

Disposal of a **main-thread-taken** cursor is deferred to the worker, while the
timeout callback replies (with the cursor id) and returns **without disposing
it**. The cursor is left busy (`pos==-1`) across the reply flush. Two existing
properties turn that into the visible bug:

- The timeout reply flushes in the same `beforeSleep`, independent of the worker
  (`redis/blocked_client_timeout_reply.md`), so the client can re-send while the
  worker is still running.
- `Cursors_TakeForExecution` collapses **busy** (`pos==-1`) and **absent** into
  the same `NULL` → "Cursor not found", so the re-send can't tell "retry, it's
  mine, still being parked" from "really gone".

## 6. Suggested fixes

**Key constraint first:** the timer **cannot** simply dispose the cursor itself
when it wins the claim. The worker is still iterating `execState` inside
`sendChunk`; making the cursor takeable (`Cursor_Pause`) at that moment would let
read#2 take it and run the **same AREQ concurrently** with read#1's worker — a
worse bug (UAF / double-use of `execState`). That is exactly why the current
design parks on the worker, *after* `sendChunk` returns. So the fix is about
**ordering the reply after the park**, not moving disposal onto the timer.

1. **Make the claim-wins branch wait for the worker's park before replying**
   (preferred, minimal). In `CursorReadTimeoutReturnStrictCallback`, even when
   the timer wins the claim, block on a "cursor parked" signal from the worker
   (the worker already runs `Cursor_Pause` in the `aggregateResultsClaimLost`
   branch at `:2020-2032`; have it post a condvar/flag there). Only then send the
   empty+cid reply. Because the timeout reply flushes *after* the callback
   returns, park-before-flush now holds, and read#2 always sees an idle cursor.
   The reply content is unchanged (empty + advertised cid); only its timing moves
   to "after the worker is done with `execState`".
2. **Distinguish busy from absent in the take** (defense in depth / alternative).
   Add a `BUSY` result to `Cursors_TakeForExecution`; on `BUSY` for a valid cid,
   briefly wait for the worker to finish parking (or reply a retry signal)
   instead of "Cursor not found". Hides the symptom without changing the
   timeout-callback ordering; could also stand in if (1) proves awkward.
3. **SyncPoint regression test.** Drive 3b with 1 worker + a sync point that
   pins the worker pre-aggregation, fire the timeout, then issue a second READ
   for the same cid and assert it does **not** get "Cursor not found".

## 7. Verification / next steps

- Confirm against a repro on `codex/MOD-15285-return-strict-hybrid-cursors`
  (standalone, RETURN_STRICT, 1 worker): the false "Cursor not found" should
  come from `RSCursorReadCommand`'s `Cursors_TakeForExecution(cid) == NULL` at
  `:2249/2257` while the worker is still mid-`sendChunk`.
- The fix belongs in the 3b callback (`CursorReadTimeoutReturnStrictCallback`)
  and/or `Cursors_TakeForExecution`; the coord path (3a) needs no change.
