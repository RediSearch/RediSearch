
// This is where the modules build/version is declared.
// If declared with -D in compile time, this file is ignored

#pragma once

#define REDISEARCH_VERSION_MAJOR 2
#define REDISEARCH_VERSION_MINOR 2
#define REDISEARCH_VERSION_PATCH 11

#ifndef REDISEARCH_MODULE_NAME
#define REDISEARCH_MODULE_NAME "search"
#endif

#define REDISEARCH_MODULE_VERSION \
  (REDISEARCH_VERSION_MAJOR * 10000 + REDISEARCH_VERSION_MINOR * 100 + REDISEARCH_VERSION_PATCH)
