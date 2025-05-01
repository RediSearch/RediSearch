/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


// This is where the modules build/version is declared.
// If declared with -D in compile time, this file is ignored

#pragma once

#define REDISEARCH_VERSION_MAJOR 8
#define REDISEARCH_VERSION_MINOR 0
#define REDISEARCH_VERSION_PATCH 0

#ifndef REDISEARCH_MODULE_NAME
#define REDISEARCH_MODULE_NAME "search"
#endif

#define REDISEARCH_MODULE_VERSION \
  (REDISEARCH_VERSION_MAJOR * 10000 + REDISEARCH_VERSION_MINOR * 100 + REDISEARCH_VERSION_PATCH)
