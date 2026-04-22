/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdlib.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <time.h>

typedef struct MRChannel MRChannel;
MRChannel *MR_NewChannel();

// Push an item to the channel. Succeeds even if the channel is closed.
void MRChannel_Push(MRChannel *chan, void *ptr);

/* Pop an item, or wait until there is an item to pop or until the channel is closed.
 * Return NULL if the channel is empty and MRChannel_Unblock was called by another thread */
void *MRChannel_Pop(MRChannel *chan);

/* Pop an item, optionally honouring a clock deadline and/or an external abort flag.
 * Parameters:
 *   - chan: the channel to pop from
 *   - abstime: absolute CLOCK_MONOTONIC_RAW-based deadline. NULL disables the clock.
 *   - abortFlag: atomic flag re-checked on every (re)entry to the wait loop. NULL
 *                disables abort-flag awareness. The caller that flips the flag must
 *                also call MRChannel_WakeAbort so a blocked reader re-evaluates it.
 *   - timedOut: output parameter (may be NULL), set to true if the function returned
 *               because the clock deadline expired.
 * With both knobs NULL this is equivalent to MRChannel_Pop.
 * Returns: the popped item, or NULL if unblocked, timed out, or aborted. */
void *MRChannel_PopWithTimeout(MRChannel *chan, const struct timespec *abstime,
                               atomic_bool *abortFlag, bool *timedOut);

/* Wake any thread currently blocked in MRChannel_PopWithTimeout so it re-evaluates
 * its abort flag. Safe to call even if no reader is blocked. */
void MRChannel_WakeAbort(MRChannel *chan);

// Same as MRChannel_Pop, but does not lock the channel nor wait for results if it's empty.
// This is unsafe, and should only be used when the caller is sure that the channel is not being used by other threads.
void *MRChannel_UnsafeForcePop(MRChannel *chan);

// Make channel unblocking for a single call to `MRChannel_Pop`.
void MRChannel_Unblock(MRChannel *chan);

size_t MRChannel_Size(MRChannel *chan);

// Free the channel. Assumes the caller has already emptied the channel.
void MRChannel_Free(MRChannel *chan);
