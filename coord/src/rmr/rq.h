/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include <stdlib.h>

typedef void (*MRQueueCallback)(void *);

#ifndef RQ_C__
typedef struct MRWorkQueue MRWorkQueue;

MRWorkQueue *RQ_New(int maxPending);

void RQ_Done(MRWorkQueue *q);

void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata);
struct MRClusterTopology;
void RQ_Push_Topology(MRWorkQueue *q, MRQueueCallback cb, struct MRClusterTopology *topo);
#endif // RQ_C__
