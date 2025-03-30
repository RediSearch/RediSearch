/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "info/info_redis/types/blocked_queries.h"

#ifdef __cplusplus
extern "C" {
#endif

// Call in module startup, initializes the thread local storage
// 0 - success, otherwise the returned int is a system error code
int MainThread_InitBlockedQueries();
// Call in module shutdown, destroys the thread local storage
void MainThread_DestroyBlockedQueries();

// Return the active queries list, will return null if called outside the main thread
BlockedQueries *MainThread_GetBlockedQueries();

#ifdef __cplusplus
}
#endif