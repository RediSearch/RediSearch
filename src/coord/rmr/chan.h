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

/* Pop an item, with optional CLOCK_MONOTONIC_RAW deadline (`abstime`) and/or abort
 * flag (re-checked on each wait entry; pair with MRChannel_WakeAbort). `timedOut`
 * set if deadline expired. At least one of `abstime` / `abortFlag` must be non-NULL;
 * callers wanting an indefinite blocking pop should use MRChannel_Pop. */
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
