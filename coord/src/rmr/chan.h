/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include <stdlib.h>
#include <stdbool.h>
#include <time.h>

typedef struct MRChannel MRChannel;
MRChannel *MR_NewChannel();

// Push an item to the channel. Succeeds even if the channel is closed.
void MRChannel_Push(MRChannel *chan, void *ptr);

/* Pop an item, or wait until there is an item to pop or until the channel is closed.
 * Return NULL if the channel is empty and MRChannel_Unblock was called by another thread */
void *MRChannel_Pop(MRChannel *chan);

/* Pop an item with a timeout. Wait until there is an item, the channel is unblocked, or timeout expires.
 * Parameters:
 *   - chan: the channel to pop from
 *   - abstime: absolute time (CLOCK_MONOTONIC) when the timeout expires. If NULL, behaves like MRChannel_Pop.
 *   - timedOut: output parameter, set to true if the function returned due to timeout
 * Returns: the popped item, or NULL if unblocked or timed out */
void *MRChannel_PopWithTimeout(MRChannel *chan, const struct timespec *abstime, bool *timedOut);

// Same as MRChannel_Pop, but does not lock the channel nor wait for results if it's empty.
// This is unsafe, and should only be used when the caller is sure that the channel is not being used by other threads.
void *MRChannel_UnsafeForcePop(MRChannel *chan);

// Make channel unblocking for a single call to `MRChannel_Pop`.
void MRChannel_Unblock(MRChannel *chan);

size_t MRChannel_Size(MRChannel *chan);

// Free the channel. Assumes the caller has already emptied the channel.
void MRChannel_Free(MRChannel *chan);
