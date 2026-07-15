/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef LOCKFREE_RECLAIM_H__
#define LOCKFREE_RECLAIM_H__

/* Safe-memory-reclamation (SMR) epoch shared by the lock-free read paths
 * (doc table, TTL table). It lets a single writer free memory that concurrent,
 * lock-free readers may still be dereferencing, without an explicit lock on the
 * read hot path.
 *
 * Model: a retired object is destroyed only once no reader can still hold a
 * pointer to it. Correctness rests on one caller-side invariant:
 *
 *   RETIRE-AFTER-UNLINK: an object is retired only *after* it has been made
 *   unreachable to readers that begin a new read section (unlinked from its
 *   chain, or its containing array replaced by a freshly published one).
 *
 * Given that, any reader that could still reach the object must already be
 * inside a read section, so it is safe to destroy the object once the active
 * reader count returns to zero.
 *
 * This is a deliberately simple, "benchmark-grade" scheme: reclamation waits
 * for a global quiescent point (zero active readers) rather than tracking
 * per-object grace periods, so a continuous stream of readers can delay
 * reclamation. Read sections here are short (one result-processor round), so
 * the pending set stays bounded in practice.
 *
 * When no reader is active (the default product configuration, where reads
 * still run under the spec read lock), retirement degrades to an immediate
 * free, so there is no added latency or memory retention. */

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle for one open read section, returned by LFReclaim_ReadBegin and
 * consumed by LFReclaim_ReadEnd. Callers must treat it as opaque. */
typedef int LFReadToken;

/* Enter a lock-free read section. Returns an opaque token that must be handed
 * to the matching LFReclaim_ReadEnd on every return path. While any thread is
 * inside a read section, retired objects are not destroyed.
 *
 * The token identifies the sharded counter slot this section bumped. Passing it
 * back to ReadEnd lets the section be closed from a *different* thread than the
 * one that opened it (e.g. a cursor scan that resumes on another worker between
 * batches) while keeping every slot non-negative. */
LFReadToken LFReclaim_ReadBegin(void);

/* Leave the lock-free read section identified by `token` (from ReadBegin).
 * Reclamation is driven by the writer side (LFReclaim_Retire /
 * LFReclaim_TryReclaim), not from here: the active-reader count is sharded, so
 * detecting "last reader to leave" would require summing every shard on this
 * per-result hot path. */
void LFReclaim_ReadEnd(LFReadToken token);

/* Retire `ptr` for deferred destruction via `dtor(ptr)`. The caller must have
 * already satisfied RETIRE-AFTER-UNLINK (see file header). If no reader is
 * currently active the object is destroyed immediately; otherwise it is queued
 * and freed once readers quiesce. */
void LFReclaim_Retire(void *ptr, void (*dtor)(void *));

/* Opportunistically drain the pending retire list. A no-op while any reader is
 * active. Writers may call this after a batch of mutations to bound the pending
 * set. */
void LFReclaim_TryReclaim(void);

#ifdef __cplusplus
}
#endif

#endif  // LOCKFREE_RECLAIM_H__
