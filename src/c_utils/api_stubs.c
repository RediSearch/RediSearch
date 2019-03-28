#include <redisearch.h>
#include <redisearch_api.h>
#include "api_stubs.h"

REDISMODULE_INIT_SYMBOLS();
REDISEARCH_API_INIT_SYMBOLS();

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k) {
  return 0;
}

uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k) {
  return 0;
}

int moduleRegisterApi(const char *funcname, void *funcptr) {
  return 0;
}
