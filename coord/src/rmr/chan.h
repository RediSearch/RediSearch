/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include <stdlib.h>

#ifndef MR_CHAN_C_
typedef struct MRChannel MRChannel;
#endif

extern void *MRCHANNEL_CLOSED;

MRChannel *MR_NewChannel(size_t max);
int MRChannel_Push(MRChannel *chan, void *ptr);
/* Pop an item, wait indefinitely or until the channel is closed for an item.
 * Return MRCHANNEL_CLOSED if the channel is closed*/
void *MRChannel_Pop(MRChannel *chan);

void *MRChannel_ForcePop(MRChannel *chan);

/* Safely wait until the channel is closed */
void MRChannel_WaitClose(MRChannel *chan);

void MRChannel_Close(MRChannel *chan);
size_t MRChannel_Size(MRChannel *chan);
size_t MRChannel_MaxSize(MRChannel *chan);
void MRChannel_Free(MRChannel *chan);
