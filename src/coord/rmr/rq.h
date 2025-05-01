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

typedef void (*MRQueueCallback)(void *);

#ifndef RQ_C__
typedef struct MRWorkQueue MRWorkQueue;

MRWorkQueue *RQ_New(int maxPending);
void RQ_UpdateMaxPending(MRWorkQueue *q, int maxPending);

void RQ_Done(MRWorkQueue *q);

void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata);
struct MRClusterTopology;
void RQ_Push_Topology(MRQueueCallback cb, struct MRClusterTopology *topo);

void RQ_Debug_ClearPendingTopo();
#endif // RQ_C__
