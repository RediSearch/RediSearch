/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include <stdlib.h>

typedef void (*MRQueueCallback)(void *);
typedef void (*MRQueueCleanUpCallback)(void *);

#ifndef RQ_C__
typedef struct MRWorkQueue MRWorkQueue;

MRWorkQueue *RQ_New(int maxPending);

void RQ_Free(MRWorkQueue *q, RedisModuleCtx *ctx);

void RQ_Done(MRWorkQueue *q);

/* if free_cb is not NULL it will be called in RQ_Free, which is invoked during a shutdown event.
Ideally, there shouldn't be **execution** requests remaining in the queue at the end of the test.
However, periodic requests might still be in the queue when we receive the shutdown event. In such cases, the
sanitizer will throw an error for direct and indirect (if the request point to additional allocated memory) leaks.
We only need a free_cb for requests that might still be in the queue during shutdown, to prevent errors in the sanitizer. */
void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata, MRQueueCleanUpCallback free_cb);
#endif // RQ_C__
