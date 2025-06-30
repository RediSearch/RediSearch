/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __MDMA_LOGGING__
#define __MDMA_LOGGING__

// Write message to redis log in debug level
void LogCallback(const char *level, const char *fmt, ...);

#endif
