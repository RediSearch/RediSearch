#include <redisearch.h>
#include <inverted_index.h>
#include <cstdio>

REDISMODULE_INIT_SYMBOLS();

extern "C" {

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k) {
  return 0;
}

uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k) {
  return 0;
}

int moduleRegisterApi(const char *funcname, void *funcptr) {
  return 0;
}
}

int main(int, char **) {
  printf("Size of document metadata: %lu\n", sizeof(RSDocumentMetadata));
  printf("Size of inverted index: %lu\n", sizeof(InvertedIndex));
  printf("Size of index block: %lu\n", sizeof(IndexBlock));
  return 0;
}
