/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


// This is where the modules build/version is declared.
// If declared with -D in compile time, this file is ignored

#pragma once

#define REDISEARCH_VERSION_MAJOR 2
#define REDISEARCH_VERSION_MINOR 6
#define REDISEARCH_VERSION_PATCH 15

#ifndef REDISEARCH_MODULE_NAME
#define REDISEARCH_MODULE_NAME "search"
#endif

#define REDISEARCH_MODULE_VERSION \
  (REDISEARCH_VERSION_MAJOR * 10000 + REDISEARCH_VERSION_MINOR * 100 + REDISEARCH_VERSION_PATCH)
