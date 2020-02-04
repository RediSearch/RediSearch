// This is where the modules build/version is declared.
// If declared with -D in compile time, this file is ignored
#ifndef REDISEARCH_MODULE_VERSION

#define REDISEARCH_VERSION_MAJOR 1
#define REDISEARCH_VERSION_MINOR 6
#define REDISEARCH_VERSION_PATCH 8

#ifndef REDISEARCH_MODULE_NAME
#define REDISEARCH_MODULE_NAME "ft"
#endif

#define REDISEARCH_MODULE_VERSION \
  (REDISEARCH_VERSION_MAJOR * 10000 + REDISEARCH_VERSION_MINOR * 100 + REDISEARCH_VERSION_PATCH)

#ifdef RS_GIT_VERSION
static inline const char* RS_GetExtraVersion() {
  return RS_GIT_VERSION;
}
#else
static inline const char* RS_GetExtraVersion() {
  return "";
}
#endif

#endif
