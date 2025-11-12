/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once
#include "info/info_redis/types/spec_info.h"

#ifdef __cplusplus
extern "C" {
#endif

// Current Thread:
// Can be any a thread working on an index spec
// For example:
// - main thread
// - indexing thread
// - gc thread
// - background query thread

// Tries to obtain the thread local info for the current thread, returns null if missing
SpecInfo* CurrentThread_TryGetSpecInfo();
// Set the current spec the current thread is working on
// If the thread will crash while pointing to this spec then the spec information will be outputted
// We require a strong ref in order to obtain some minimal information on the spec if it is deleted while the thread is working on it
void CurrentThread_SetIndexSpec(StrongRef specRef);
// Clear the current index spec the thread is working on
void CurrentThread_ClearIndexSpec();

#ifdef __cplusplus
}
#endif