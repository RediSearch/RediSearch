/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1).
 */

// RediSearchDisk version - independent from RediSearch OSS
// This file is the single source of truth for release versioning.

#pragma once

#define REDISEARCH_VERSION_MAJOR 99
#define REDISEARCH_VERSION_MINOR 99
#define REDISEARCH_VERSION_PATCH 99

#ifndef REDISEARCH_MODULE_NAME
#define REDISEARCH_MODULE_NAME "search"
#endif

#define REDISEARCH_MODULE_VERSION \
  (REDISEARCH_VERSION_MAJOR * 10000 + REDISEARCH_VERSION_MINOR * 100 + REDISEARCH_VERSION_PATCH)

