// This is where the modules build/version is declared.
// If declared with -D in compile time, this file is ignored
#ifndef REDISEARCH_MODULE_VERSION

#define REDISEARCH_VERSION_MAJOR 0
#define REDISEARCH_VERSION_MINOR 99
#define REDISEARCH_VERSION_PATCH 0

#define REDISEARCH_MODULE_VERSION \
  (REDISEARCH_VERSION_MAJOR * 10000 + REDISEARCH_VERSION_MINOR * 100 + REDISEARCH_VERSION_PATCH)

#endif
