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
#include <stddef.h>

typedef void (*MRQueueCallback)(void *);

typedef struct MRWorkQueue MRWorkQueue;

MRWorkQueue *RQ_New(int maxPending, size_t id);
void RQ_Free(MRWorkQueue *q);

void RQ_UpdateMaxPending(MRWorkQueue *q, int maxPending);

void RQ_Done(MRWorkQueue *q);

void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata);
struct MRClusterTopology;
void RQ_Push_Topology(MRWorkQueue *q, MRQueueCallback cb, struct MRClusterTopology *topo);

void RQ_Debug_ClearPendingTopo(MRWorkQueue *q);

const void* RQ_GetLoop(const MRWorkQueue *q);

size_t RQ_GetMaxPending(const MRWorkQueue *q);
